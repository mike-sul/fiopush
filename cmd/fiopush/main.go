package main

import (
	"flag"
	"foundriesio/ostreehub/pkg/fiopush"
	"log"
	"os"
)

var (
	DefaultServerUrl = "https://api.foundries.io/ota/ostreehub"
)

func main() {
	cwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	repo := flag.String("repo", cwd, "A path to an ostree repo")
	ostreeHubUrl := flag.String("server", DefaultServerUrl, "An URL to OSTree Hub to upload repo to")
	factory := flag.String("factory", "", "A Factory to upload repo for")
	creds := flag.String("creds", "", "A credential archive with auth material")
	flag.Parse()

	var pusher fiopush.Pusher
	if *creds != "" {
		pusher, err = fiopush.NewPusher(*repo, *creds)
	} else {
		pusher, err = fiopush.NewPusherNoAuth(*repo, *ostreeHubUrl, *factory)
	}
	if err != nil {
		log.Fatalf("Failed to create Fio Pusher: %s\n", err.Error())
	}

	if err := pusher.Run(); err != nil {
		log.Fatalf("Failed to run Fio Pusher: %s\n", err.Error())
	}

	log.Printf("Pushing %s to %s, factory: %s ...\n", *repo, pusher.HubUrl(), pusher.Factory())
	report, err := pusher.Wait()
	if err != nil {
		log.Fatalf("Failed to push repo: %s\n", err.Error())
	}

	log.Printf("Checked: %d\n", report.Checked)
	log.Printf("Sent %d files, %d objects, %d bytes\n", report.Sent.FileNumb, report.Sent.ObjNumb, report.Sent.Bytes)
	log.Printf("Uploaded %d files, synced %d objects, uploaded to GCS %d objects\n",
		report.Synced.UploadedFileNumb, report.Synced.SyncedFileNumb, report.Synced.UploadSyncedFileNumb)
	log.Printf("Failed to sync %d objects", report.Synced.SyncFailedNumb)
}
