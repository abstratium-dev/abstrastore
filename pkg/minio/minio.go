package minio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"

	"github.com/abstratium-informatique-sarl/abstrastore/pkg/schema"
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

// Param: entity - the address of a struct that should be marshalled to JSON and persisted
// Param: id - the id of the entity to insert
// sql: insert into table_name (column1, column2, column3, ...) values (value1, value2, value3, ...)
//  TODO use reflection to access the ID
func (r *MinioRepository) InsertIntoTable(ctx context.Context, table schema.Table, entity any, id string) error {
	data, err := json.Marshal(entity)
	if err != nil {
		return err
	}
	opts := minio.PutObjectOptions{
		ContentType: "application/json",
	}
	_, err = r.Client.PutObject(ctx, r.BucketName, table.Path(id), bytes.NewReader(data), int64(len(data)), opts)
	if err != nil {
		return err
	}

	// handle indices
	// use the path as a tree style index. that way, we simply walk down the tree until we find the key
	for _, index := range table.Indices {
		// use reflection to fetch the value of the field that the index is based on
		value, err := getFieldValueAsString(entity, index.Field)
		if err != nil {
			return err
		}

		data := []byte(table.Path(id)) // store the path to the actual record, i.e. using the primary key
		opts := minio.PutObjectOptions{
			ContentType: "text/plain",
		}

		indexPath := index.Path(value)
		_, err = r.Client.PutObject(ctx, r.BucketName, indexPath, bytes.NewReader(data), int64(len(data)), opts)
		if err != nil {
			return err
		}
	}

	return nil
}

// sql: select * from table_name where column1 = value1 (column1 is in an index)
func (r *MinioRepository) SelectFromTableWhereIndexedFieldEquals(ctx context.Context, table schema.Table, field string, value string, template any) error {
	// use the index definition to find the path of the actual record containing the data that the caller wants to read
	index, err := table.GetIndex(field)
	if err != nil {
		return err
	}
	indexPath := index.Path(value)
	indexData, err := r.Client.GetObject(ctx, r.BucketName, indexPath, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer indexData.Close()

	path, err := io.ReadAll(indexData)
	if err != nil {
		return err
	}
	
	// read the actual record
	recordData, err := r.Client.GetObject(ctx, r.BucketName, string(path), minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer recordData.Close()
	
	// parse the actual record
	b, err := io.ReadAll(recordData)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, template); err != nil {
		return err
	}
	return nil
}

func getFieldValueAsString(obj interface{}, fieldName string) (string, error) {
	v := reflect.ValueOf(obj)

	// If it's a pointer, get the value it points to
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	// Make sure we're dealing with a struct
	if v.Kind() != reflect.Struct {
		return "", fmt.Errorf("expected a struct, got %s", v.Kind())
	}

	// Get the field by name
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return "", fmt.Errorf("no such field: %s", fieldName)
	}

	// check it is a string, otherwise create an error
	if field.Kind() != reflect.String {
		return "", fmt.Errorf("field %s is not a string", fieldName)
	}

	return field.Interface().(string), nil
}
