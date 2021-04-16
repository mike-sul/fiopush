package fiopush

import (
	"bytes"
	"encoding/json"
	"fmt"
	"foundriesio/ostreehub/pkg/oshub"
	"hash/crc32"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
)

type (
	Pusher interface {
		HubUrl() string
		Factory() string

		Run() error
		Wait() (*Report, error)
	}

	Status struct {
		Check <-chan uint
		Send  <-chan *oshub.SendReport
		Sync  <-chan *oshub.SyncReport
	}

	Report struct {
		Checked uint
		Sent    oshub.SendReport
		Synced  oshub.SyncReport
	}
)

type (
	pusher struct {
		repo   string
		url    *url.URL
		hub    *OSTreeHub
		token  string
		status *Status
	}
)

const (
	// a single goroutine traverses an ostree repo,
	// generates CRC for each file and enqueue a file info to the queue/channel
	walkQueueSize uint = 10000
	// a number of goroutine to read from the file queue and push them to OSTreeHub
	// each goroutine at first checks if given files are already present on GCS and uploads
	// only those files/objects that are missing or CRC is not equal
	concurrentPusherNumb int = 20
	// maximum number of files to check per a single HTTP request
	filesToCheckMaxNumb int = oshub.FilesToCheckMaxNumb
)

var (
	repoFileFilterIn = []string{
		"./objects/",
		"./config",
		"./refs/",
	}
)

func NewPusher(repo string, credFile string) (Pusher, error) {
	if err := checkRepoDir(repo); err != nil {
		return nil, err
	}
	hub, err := ExtractUrlAndFactory(credFile)
	if err != nil {
		return nil, err
	}
	reqUrl, err := url.Parse(hub.URL + "/ota/ostreehub/" + hub.Factory + "/v1/repos/lmp")
	if err != nil {
		return nil, err
	}
	return &pusher{repo: repo, url: reqUrl, hub: hub, token: ""}, nil
}

func NewPusherNoAuth(repo string, hubURL string, factory string) (Pusher, error) {
	if err := checkRepoDir(repo); err != nil {
		return nil, err
	}
	if hubURL == "" {
		return nil, fmt.Errorf("URL to OSTreehub is not specified")
	}
	if factory == "" {
		return nil, fmt.Errorf("factory name is not specified")
	}
	hub := OSTreeHub{
		URL:     hubURL,
		Factory: factory,
	}
	reqUrl, err := url.Parse(hub.URL + "/v1/repos/lmp?factory=" + hub.Factory)
	if err != nil {
		return nil, err
	}
	return &pusher{repo: repo, url: reqUrl, hub: &hub, token: ""}, nil
}

func (p *pusher) HubUrl() string {
	return p.hub.URL
}

func (p *pusher) Factory() string {
	return p.hub.Factory
}

func (p *pusher) Run() error {
	if err := p.auth(); err != nil {
		return err
	}

	if p.status != nil {
		return fmt.Errorf("cannot run Pusher if there are unfinished push jobs")
	}
	p.status = push(p.repo, walkAndCrcRepo(p.repo), p.url, p.token)
	return nil
}

func (p *pusher) Wait() (*Report, error) {
	if p.status == nil {
		return nil, fmt.Errorf("cannot wait for Pusher jobs completion if there are none of running jobs")
	}
	return wait(p.status), nil
}

func checkRepoDir(dir string) error {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("The specified directory doesn't exist: %s\n", dir)
	}
	if _, err := os.Stat(path.Join(dir, "config")); os.IsNotExist(err) {
		return fmt.Errorf("The specified directory doesn't contain an ostree repo: %s\n", dir)
	}
	if _, err := os.Stat(path.Join(dir, "objects")); os.IsNotExist(err) {
		return fmt.Errorf("The specified directory doesn't contain ostree repo objects: %s\n", dir)
	}
	return nil
}

func (p *pusher) auth() error {
	if p.hub.Auth == nil {
		return nil
	}
	t, err := GetOAuthToken(p.hub.Auth)
	if err != nil {
		return err
	}
	log.Printf("OAuth token has been successfully obtained at %s\n", p.hub.Auth.Server)
	p.token = t
	return nil
}

func walkAndCrcRepo(repoDir string) <-chan *oshub.RepoFile {
	dir := filepath.Clean(repoDir)
	queue := make(chan *oshub.RepoFile, walkQueueSize)
	go func() {
		defer close(queue)
		table := crc32.MakeTable(crc32.Castagnoli)
		hasher := crc32.New(table)

		if err := filepath.Walk(dir, func(fullPath string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				log.Fatalf("Failed to walk through a repo: %s\n", walkErr.Error())
			}
			if info.IsDir() {
				return nil
			}
			relPath := strings.Replace(fullPath, dir, ".", 1)
			if !filterRepoFiles(relPath) {
				return nil
			}

			f, err := os.Open(fullPath)
			if err != nil {
				log.Fatalf("Failed to open file: %s\n", err.Error())
			}
			defer func() {
				if err := f.Close(); err != nil {
					panic(err)
				}
			}()

			hasher.Reset()
			w, err := io.Copy(hasher, f)
			if err != nil {
				log.Fatalf("Failed to write file data to CRC hasher: %s\n", err.Error())
			}
			if w != info.Size() {
				log.Fatalf("Invalid amount of data written to CRC hasher: %s\n", err.Error())
			}
			crc := hasher.Sum32()
			queue <- &oshub.RepoFile{Path: relPath, CRC32: crc}
			return nil
		}); err != nil {
			log.Fatalf("Failed to walk through a repo directory: %s\n", err.Error())
		}
	}()
	return queue
}

func filterRepoFiles(path string) bool {
	for _, f := range repoFileFilterIn {
		if strings.HasPrefix(path, f) {
			return true
		}
	}
	return false
}

func push(repoDir string, fileQueue <-chan *oshub.RepoFile, url *url.URL, token string) *Status {
	checkReportQueue := make(chan uint, concurrentPusherNumb)
	reportQueue := make(chan *oshub.SendReport, concurrentPusherNumb)
	recvReportQueue := make(chan *oshub.SyncReport, concurrentPusherNumb)

	go func() {
		var wg sync.WaitGroup
		for ii := 0; ii < concurrentPusherNumb; ii++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					objectsToCheck := make(map[string]uint32)

					for object := range fileQueue {
						objectsToCheck[object.Path] = object.CRC32
						if len(objectsToCheck) > filesToCheckMaxNumb {
							break
						}
					}

					if len(objectsToCheck) == 0 {
						break
					}

					objectsToSync := checkRepo(objectsToCheck, url, token)

					checkReportQueue <- uint(len(objectsToCheck))

					if len(objectsToSync) > 0 {
						tarReader, sendReportChannel := oshub.Tar(repoDir, objectsToSync)
						recvReportChannel := pushRepo(tarReader, url, token)

						reportQueue <- <-sendReportChannel
						recvReportQueue <- <-recvReportChannel
					}
				}
			}()
		}
		wg.Wait()
		close(checkReportQueue)
		close(reportQueue)
		close(recvReportQueue)
	}()
	return &Status{Check: checkReportQueue, Send: reportQueue, Sync: recvReportQueue}
}

func checkRepo(objs map[string]uint32, url *url.URL, token string) map[string]uint32 {
	jsonObjects, _ := json.Marshal(objs)
	req, err := http.NewRequest("GET", url.String(), bytes.NewBuffer(jsonObjects))
	if err != nil {
		log.Fatalf("Failed to create a request to check objects presence: %s\n", err.Error())
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("Failed to make request to check objects presence: %s\n", err.Error())
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("Failed to close a response body: %s\n", err.Error())
		}
	}()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Failed to read response: %s\n", err.Error())
	}

	respMap := map[string]uint32{}
	if err := json.Unmarshal(body, &respMap); err != nil {
		log.Fatalf("Failed to read response: %s\n", err.Error())
	}
	return respMap
}

func pushRepo(pr *io.PipeReader, u *url.URL, token string) <-chan *oshub.SyncReport {
	req := &http.Request{
		Method:           "PUT",
		ProtoMajor:       1,
		ProtoMinor:       1,
		URL:              u,
		TransferEncoding: []string{"chunked"},
		Body:             pr,
		Header:           make(map[string][]string),
	}
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))

	//TODO: timeout
	client := &http.Client{}
	client.Transport = &http.Transport{DisableCompression: false,
		WriteBufferSize: 1024 * 1025 * 10, ReadBufferSize: 1024 * 1024 * 10}

	reportChannel := make(chan *oshub.SyncReport, 1)
	go func() {
		defer close(reportChannel)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		} else {
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Printf("Filed to read response: %s\n", err.Error())
			}
			var status oshub.SyncReport
			if err := json.Unmarshal(body, &status); err != nil {
				log.Printf("Filed to umarshal response: %s\n", err.Error())
			}
			reportChannel <- &status
		}
	}()
	return reportChannel
}

func wait(statusQueue *Status) *Report {
	var totalChecked uint
	var totalSendReport oshub.SendReport
	var totalRecvReport oshub.SyncReport
	for {
		select {
		case checked, ok := <-statusQueue.Check:
			if !ok {
				continue
			}
			totalChecked += checked
			log.Printf("Checked: %d\n", totalChecked)

		case sendReport, ok := <-statusQueue.Send:
			if !ok || sendReport == nil {
				continue
			}
			totalSendReport.FileNumb += sendReport.FileNumb
			totalSendReport.ObjNumb += sendReport.ObjNumb
			totalSendReport.Bytes += sendReport.Bytes
			log.Printf("Sent: %d\n", totalSendReport.FileNumb)

		case recvReport, ok := <-statusQueue.Sync:
			if !ok {
				log.Println("Repo sync has completed")
				return &Report{totalChecked, totalSendReport, totalRecvReport}
			}
			totalRecvReport.UploadedFileNumb += recvReport.UploadedFileNumb
			totalRecvReport.SyncedFileNumb += recvReport.SyncedFileNumb
			totalRecvReport.UploadSyncedFileNumb += recvReport.UploadSyncedFileNumb
			totalRecvReport.SyncFailedNumb += recvReport.SyncFailedNumb
		}
	}
}
