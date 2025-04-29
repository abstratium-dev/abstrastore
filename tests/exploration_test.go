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

func TestExplorationDeleteFilesWithVersioningOn(t *testing.T) {
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	repo := getRepo()

	repo.Client.TraceOn(log.Writer())
	repo.Client.TraceErrorsOnlyOff()

	path := "test-001/a"

	// clean
	err := repo.Client.RemoveObject(context.Background(), repo.BucketName, path, minio.RemoveObjectOptions{
		ForceDelete: true,
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Object cleaned")

	// create an object
	data := []byte("a")
	uploadInfo, err := repo.Client.PutObject(context.Background(), repo.BucketName, path, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "text/plain",
		UserMetadata: map[string]string{
			"a": "b",
		},
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Object uploaded: %+v\n", uploadInfo)

	// update same object
	data = []byte("c")
	uploadInfo, err = repo.Client.PutObject(context.Background(), repo.BucketName, path, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "text/plain",
		UserMetadata: map[string]string{
			"a": "c",
		},
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Object updated: %+v\n", uploadInfo)
	versionIDOfSecondObjectVersion := uploadInfo.VersionID


	// update same object again
	data = []byte("d")
	uploadInfo, err = repo.Client.PutObject(context.Background(), repo.BucketName, path, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{
		ContentType: "text/plain",
		UserMetadata: map[string]string{
			"a": "d",
		},
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("Object uploaded again: %+v\n", uploadInfo)

	// delete the middle object
	err = repo.Client.RemoveObject(context.Background(), repo.BucketName, path, minio.RemoveObjectOptions{
		VersionID: versionIDOfSecondObjectVersion,
	})
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Middle object deleted")

	// list objects
	objects := repo.Client.ListObjects(context.Background(), repo.BucketName, minio.ListObjectsOptions{
		Prefix:     "test-001/a", // full path of object we are reading
		WithVersions: true, // get version info so that we can find the object with a timestamp before the transaction started
		ReverseVersions: false, // latest first
		WithMetadata: true,
	})
	i := 0
	for object := range objects {
		if object.Err != nil {
			log.Fatalln(object.Err)
		}
		fmt.Printf("%d -> %t -> %s -> %s -> %s -> %s -> %t\n", i, object.IsLatest, object.LastModified.Format(time.RFC3339), object.VersionID, object.ETag, object.UserMetadata, object.IsDeleteMarker)
		i++

/*
		err = repo.Client.RemoveObject(context.Background(), repo.BucketName, path, minio.RemoveObjectOptions{
			VersionID: object.VersionID,
		})
		if err != nil {
			log.Fatalln(err)
		}
		fmt.Println("Object deleted")
*/
	}
	
	
	// now they are all deleted, list them again - does this work or do we need to use the parent folder as a prefix?
	objects = repo.Client.ListObjects(context.Background(), repo.BucketName, minio.ListObjectsOptions{
		Prefix:     "test-001/a", // full path of object we are reading
		WithVersions: true, // get version info so that we can find the object with a timestamp before the transaction started
		WithMetadata: true,
	})
	i = 0
	for object := range objects {
		if object.Err != nil {
			log.Fatalln(object.Err)
		}
		fmt.Printf("%d -> %t -> %s -> %s -> %s -> %s -> %t\n", i, object.IsLatest, object.LastModified.Format(time.RFC3339), object.VersionID, object.ETag, object.UserMetadata, object.IsDeleteMarker)
		i++
	}

	fmt.Println("done")
}
