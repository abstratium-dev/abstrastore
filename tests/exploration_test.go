package minio

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
)

func TestExploration(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)

	// assert := assert.New(t)

	repo := getRepo()

	err := repo.Client.EnableVersioning(context.Background(), repo.BucketName)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("2 Versioning enabled for bucket '" + repo.BucketName + "'")

	versioningConfig, err := repo.Client.GetBucketVersioning(context.Background(), repo.BucketName)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("3 Versioning configuration: %+v\n", versioningConfig)


	// create an object
	data := []byte("test")
	uploadInfo, err := repo.Client.PutObject(context.Background(), repo.BucketName, "test-001", bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("4 Object uploaded: %+v\n", uploadInfo)

	defer repo.Client.RemoveObject(context.Background(), repo.BucketName, "test-001", minio.RemoveObjectOptions{})

	// list objects
	objects := repo.Client.ListObjects(context.Background(), repo.BucketName, minio.ListObjectsOptions{
		Prefix: "test-001", // full path of object we are reading
		WithVersions: true, // get version info so that we can find the object with a timestamp before the transaction started
	})
	for object := range objects {
		if object.Err != nil {
			log.Fatalln(object.Err)
		}
		fmt.Println(object.LastModified.Format(time.RFC3339) + " -> " + object.VersionID)
	}
}

ok use versions for mvcc

any time we read, and determine that there is more than one version, and older versions are older than the configured time,
we can delete them, since they are garbage and no longer necessary for read isolation.



