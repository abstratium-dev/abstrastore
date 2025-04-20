package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

var repo *MinioRepository

type MinioRepository struct {
	Client     *minio.Client
	BucketName string

	// no need for any locks - see https://github.com/minio/minio-go/issues/1125, which seems to have fixed any issues related to goroutine-safety
}

func Setup() {
	endpoint := os.Getenv("MINIO_URL")
	accessKey := os.Getenv("MINIO_ACCESS_KEY_ID")
	secretKey := os.Getenv("MINIO_SECRET_ACCESS_KEY")
	bucketName := os.Getenv("MINIO_BUCKET_NAME")
	useSslString := os.Getenv("MINIO_USE_SSL")
	useSsl, err := strconv.ParseBool(useSslString)

	if endpoint == "" || accessKey == "" || secretKey == "" || bucketName == "" || err != nil {
		panic(fmt.Sprintf("Missing / wrong MinIO environment variables: MINIO_URL=%s, MINIO_ACCESS_KEY_ID=%s, MINIO_SECRET_ACCESS_KEY=%s, MINIO_BUCKET_NAME=%s, MINIO_USE_SSL=%s, err=%v", endpoint, accessKey, secretKey, bucketName, useSslString, err))
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSsl,
	})
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize MinIO client: %v", err))
	}

	repo = newMinioRepository(client, bucketName)
}

func newMinioRepository(client *minio.Client, bucketName string) *MinioRepository {
    return &MinioRepository{
        Client:     client,
        BucketName: bucketName,
    }
}

func GetRepository() *MinioRepository {
	return repo
}

func (r *MinioRepository) CreateFile(ctx context.Context, path string, contents []byte) error {
	_, err := r.Client.PutObject(
		ctx,
		r.BucketName,
		path,
		bytes.NewReader(contents),
		int64(len(contents)),
		minio.PutObjectOptions{ContentType: "application/json"},
	)
	return err
}

func (r *MinioRepository) ReadFile(ctx context.Context, path string) ([]byte, error) {
	object, err := r.Client.GetObject(ctx, r.BucketName, path, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	return io.ReadAll(object)
}

func (r *MinioRepository) UpdateFile(ctx context.Context, path string, contents []byte) error {
	// no locking required, because CreateFile does that itself
	return r.CreateFile(ctx, path, contents)
}

func (r *MinioRepository) DeleteFile(ctx context.Context, path string) error {
	return r.Client.RemoveObject(ctx, r.BucketName, path, minio.RemoveObjectOptions{})
}

func (r *MinioRepository) DeleteFolder(ctx context.Context, folderPrefix string) error {
	// thanks to gemini:

	// Channel to hold object names to be removed
	objectsCh := make(chan minio.ObjectInfo)

	// Goroutine to list objects and send them to the channel
	go func() {
		defer close(objectsCh) // Close the channel when listing is done
		// ListObjectsOptions recursive defaults to false if not set.
		// Set Recursive to true to find objects in sub-"folders".
		listOpts := minio.ListObjectsOptions{
			Prefix:    folderPrefix,
			Recursive: true,
		}
		for object := range r.Client.ListObjects(ctx, r.BucketName, listOpts) {
			if object.Err != nil {
				panic(object.Err)
			} else {
				objectsCh <- object
			}
		}
	}() // End of goroutine

	// --- Perform Bulk Deletion ---
	// RemoveObjectsOptions can be adjusted if needed (e.g., for GovernanceBypass)
	opts := minio.RemoveObjectsOptions{
		// GovernanceBypass: true, // Uncomment if dealing with locked objects under governance mode
	}

	// RemoveObjects consumes the objects from the channel
	errorCh := r.Client.RemoveObjects(ctx, r.BucketName, objectsCh, opts)

	// --- Process Deletion Errors ---
	errors := make([]minio.RemoveObjectError, 0, 10)
	// Drain the error channel and log any errors
	for e := range errorCh {
		errors = append(errors, e)
	}

	if len(errors) > 0 {
		return fmt.Errorf("Finished deletion process with %d errors. First one was %w", len(errors), errors[0].Err)
	}
	return nil
}

func (r *MinioRepository) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	var files []string
	objectCh := r.Client.ListObjects(ctx, r.BucketName, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: true,
	})
	for object := range objectCh {
		if object.Err != nil {
			return nil, object.Err
		}
		files = append(files, object.Key)
	}
	return files, nil
}

func (r *MinioRepository) GetReader(ctx context.Context, path string) (io.ReadCloser, error) {
	return r.Client.GetObject(ctx, r.BucketName, path, minio.GetObjectOptions{})
}
