package oshub

import (
	"archive/tar"
	"fmt"
	"github.com/labstack/echo/v4"
	"io"
	"os"
	"path"
	"strconv"
	"strings"
)

func Untar(tarReader *tar.Reader, dstDir string, l echo.Logger) <-chan *RepoFile {
	fileQueue := make(chan *RepoFile, 100)
	logger := l

	go func() {
		defer func() {
			err := recover()
			if err != nil {
				// TODO: done/close channel
				logger.Errorf("Failed to process an input TAR stream: %s\n", err)
			}
		}()

		defer close(fileQueue)
		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				panic("failed to read an input TAR stream: " + err.Error())
			}

			name := header.Name
			switch header.Typeflag {
			case tar.TypeDir:
				d := path.Join(dstDir, name)
				err := os.MkdirAll(d, 0755)
				if err != nil {
					panic("failed to create a directory: " + d + " " + err.Error())
				}
				continue

			case tar.TypeReg:
				p := path.Join(dstDir, name)
				d := path.Dir(p)
				err := os.MkdirAll(d, 0755)
				if err != nil {
					panic("failed to create a directory: " + d + " " + err.Error())
				}
				f, err := os.Create(p)
				if err != nil {
					panic("failed to create a file: " + p + " " + err.Error())
				}
				_, err = io.Copy(f, tarReader)
				if err != nil {
					f.Close()
					panic("failed to copy a file: " + p + " " + err.Error())
				}
				f.Close()
				expectedCrc, err := strconv.ParseUint(header.PAXRecords["FIO.ostree.CRC"], 10, 0)
				if err != nil {
					expectedCrc = 0
				}
				fileQueue <- &RepoFile{Path: name, CRC32: uint32(expectedCrc)}
			default:
				panic("failed to read an input TAR stream")
			}
		}
	}()

	return fileQueue
}

func Tar(repoDir string, files map[string]uint32) (*io.PipeReader, <-chan *SendReport) {
	pr, pw := io.Pipe()
	reportChannel := make(chan *SendReport, 1)
	go func() {
		defer pw.Close()
		tw := tar.NewWriter(pw)
		defer tw.Close()
		defer close(reportChannel)
		var sr SendReport
		for file, crc := range files {
			f, err := os.Open(path.Join(repoDir, file))
			if err != nil {
				panic(err)
			}
			fileInfo, err := f.Stat()
			if err != nil {
				panic(err)
			}
			hdr, err := tar.FileInfoHeader(fileInfo, "")
			if err != nil {
				panic(err)
			}
			hdr.Name = file
			hdr.Format = tar.FormatPAX
			//paxRec := map[string]string{"FIO.ostree.CRC": strconv.FormatUint(uint64(crc), 10)}
			hdr.PAXRecords = map[string]string{"FIO.ostree.CRC": strconv.FormatUint(uint64(crc), 10)}
			if err := tw.WriteHeader(hdr); err != nil {
				panic(err)
			}
			if fileInfo.IsDir() {
				f.Close()
				continue
			}
			w, err := io.Copy(tw, f)
			if err != nil {
				f.Close()
				fmt.Printf(">>>>>>>>>>> PANIC: %s\n", err.Error())
				panic(err)
			}
			tw.Flush()
			f.Close()

			if strings.HasPrefix(file, "./objects") {
				sr.ObjNumb += 1
			}
			sr.FileNumb += 1
			sr.Bytes += w
		}
		reportChannel <- &sr
	}()
	return pr, reportChannel
}
