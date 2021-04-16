package oshub

import (
	gcs "cloud.google.com/go/storage"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
)

type (
	RepoFile struct {
		Path  string
		CRC32 uint32
	}

	SendReport struct {
		FileNumb uint
		ObjNumb  uint
		Bytes    int64
	}

	SyncReport struct {
		UploadedFileNumb     uint32 `json:"uploaded"`
		SyncedFileNumb       uint32 `json:"synced"`
		UploadSyncedFileNumb uint32 `json:"upload_synced"`
		SyncFailedNumb       uint32 `json:"sync_failed"`
	}
)

const (
	FilesToCheckMaxNumb int = 500
)

type (
	uploadStatus struct {
		Object *string
		Exist  bool
		Err    string
	}
)

var (
	uploader struct {
		ctx        context.Context
		client     *gcs.Client
		bucket     *gcs.BucketHandle
		bucketName string
		workerNumb int
	}
)

func InitUploader(bucket string, workerNumb int) {
	uploader.ctx = context.Background()
	client, err := gcs.NewClient(uploader.ctx)
	if err != nil {
		panic(err)
	}

	uploader.client = client
	uploader.bucketName = bucket
	uploader.bucket = uploader.client.Bucket(bucket)
	uploader.workerNumb = workerNumb
	// TODO : check access permissions
}

func Bucket() string {
	return uploader.bucketName
}

func Check(fileQueue <-chan *RepoFile, objectPrefix string) <-chan *RepoFile {
	objToSyncCh := make(chan *RepoFile, FilesToCheckMaxNumb)
	go func() {
		var wg sync.WaitGroup
		for ii := 0; ii < uploader.workerNumb; ii++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for file := range fileQueue {
					if !strings.HasPrefix(file.Path, "./objects/") {
						// upload ./refs and ./config by default
						objToSyncCh <- file
						continue
					}

					objectName := objectPrefix + file.Path[len("./objects/")-1:]
					obj := uploader.bucket.Object(objectName)
					attr, err := obj.Attrs(uploader.ctx)
					if err != nil {
						if err != gcs.ErrObjectNotExist {
							fmt.Printf("Object doesn't exists: %s\n, err: %s\n", objectName, err.Error())
						} else {
							fmt.Printf("Failed to query GCS: %s\n, err: %s\n", objectName, err.Error())
						}
						objToSyncCh <- file
						continue
					}

					if file.CRC32 != attr.CRC32C {
						fmt.Printf("CRC doesn't match: %s,  %d vs %d\n", objectName, file.CRC32, attr.CRC32C)
						objToSyncCh <- file
						continue
					}
				}
			}()
		}
		wg.Wait()
		close(objToSyncCh)
	}()
	return objToSyncCh
}

func Filter(fileQueue <-chan *RepoFile, filterPrefix string) (<-chan *RepoFile, <-chan uint32) {
	// filter and recv status
	objectQueue := make(chan *RepoFile, 100)
	reportQueue := make(chan uint32, 1)
	go func() {
		defer close(reportQueue)
		defer close(objectQueue)
		var fileNumb uint32 = 0
		for file := range fileQueue {
			fileNumb += 1
			if strings.HasPrefix(file.Path, filterPrefix) {
				objectQueue <- file
			}
		}
		reportQueue <- fileNumb
	}()
	return objectQueue, reportQueue
}

func Sync(objectQueue <-chan *RepoFile, objectPrefix string, srcDir string) <-chan *uploadStatus {
	statusQueue := make(chan *uploadStatus, uploader.workerNumb*100)
	go func() {
		defer close(statusQueue)
		var wg sync.WaitGroup
		for i := 0; i < uploader.workerNumb; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for object := range objectQueue {
					objectName := objectPrefix + object.Path[len("./objects/")-1:]
					srcFilePath := path.Join(srcDir, object.Path)
					statusQueue <- upload(objectName, object, srcFilePath)
				}
			}()
		}
		wg.Wait()
	}()
	return statusQueue
}

func Wait(reportQueue <-chan uint32, statusQueue <-chan *uploadStatus) *SyncReport {
	var status SyncReport
	for {
		select {
		case fileNumb, ok := <-reportQueue:
			if ok {
				status.UploadedFileNumb = fileNumb
			}
		case uploadStatus, ok := <-statusQueue:
			if !ok {
				return &status
			}
			status.SyncedFileNumb += 1
			if uploadStatus.Err != "" {
				status.SyncFailedNumb += 1
			}
			if !uploadStatus.Exist {
				status.UploadSyncedFileNumb += 1
			}
		} //select
	} // for
}

func upload(objectName string, object *RepoFile, srcFilePath string) *uploadStatus {
	// TODO: log error messages to Echo logger and return a list of failed objects along with failure reason to a client
	obj := uploader.bucket.Object(objectName)
	attr, err := obj.Attrs(uploader.ctx)
	if err == nil && attr.CRC32C == object.CRC32 {
		return &uploadStatus{Object: &object.Path, Exist: true}
	}

	if err != nil && err != gcs.ErrObjectNotExist {
		//fmt.Printf("invalid object state: %s\n", objectName)
		return &uploadStatus{Object: &object.Path, Exist: false, Err: err.Error()}
	}

	f, err := os.Open(srcFilePath)
	if err != nil {
		//fmt.Printf("failed to open: %s\n", srcFilePath)
		return &uploadStatus{Object: &object.Path, Exist: false, Err: err.Error()}
	}
	defer f.Close()

	// TODO:  upload by talking directly to GCS REST API. There is some memory leaking issue here
	//https://github.com/googleapis/google-cloud-go/issues/1380
	w := obj.NewWriter(uploader.ctx)
	if w == nil {
		fmt.Printf("failed to create a writer for: %s\n", objectName)
		return &uploadStatus{Object: &object.Path, Exist: false, Err: "failed to create a bucket object writer"}
	}
	fmt.Printf("Uploading an object to GCS bucket: %s\n", objectName)
	if object.CRC32 != 0 {
		w.SendCRC32C = true
		w.CRC32C = object.CRC32
	}
	w.ChunkSize = 0
	size, err := io.Copy(w, f)
	if err != nil {
		fmt.Printf("failed to copy for: %s\n", objectName)
		return &uploadStatus{Object: &object.Path, Exist: false, Err: err.Error()}
	}

	err = w.Close()
	if err != nil {
		fmt.Printf("failed to close/flush writing to the bucket for: %s\n%s\n", objectName, err.Error())
		return &uploadStatus{Object: &object.Path, Exist: false, Err: err.Error()}
	}

	fmt.Printf("Successfully uploaded %d to GCS bucket\n", size)
	return &uploadStatus{Object: &object.Path, Exist: false}
}
