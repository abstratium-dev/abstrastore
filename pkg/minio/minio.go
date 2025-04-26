package minio

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/abstratium-informatique-sarl/abstrastore/internal/util"
	"github.com/abstratium-informatique-sarl/abstrastore/pkg/schema"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const ROLLBACK_ID = "rollbackId"

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

	// ensure versioning is enabled
	versioningConfig, err := repo.Client.GetBucketVersioning(context.Background(), repo.BucketName)
	if err != nil {
		panic(fmt.Sprintf("Failed to get bucket versioning configuration: %v", err))
	}
	if !versioningConfig.Enabled() {
		panic("Versioning is not enabled for the bucket")
	}
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

type DuplicateKeyError struct {
	Details string
}

func (e *DuplicateKeyError) Error() string {
	return e.Details
}

type NoSuchKeyError struct {
	Details string
}

func (e *NoSuchKeyError) Error() string {
	return e.Details
}

type TypedQuery[T any] struct {
	ctx      context.Context
	template *T
	repo     *MinioRepository
}

// Param: repo - the repository to use
// Param: ctx - the context to use
// Param: template - the template of type T, used to unmarshal the JSON data
// Returns: a new TypedQuery[T] instance
func NewTypedQuery[T any](repo *MinioRepository, ctx context.Context, template *T) TypedQuery[T] {
	return TypedQuery[T]{
		ctx:      ctx,
		template: template,
		repo:     repo,
	}
}

type ContextContainer[T any] struct {
	ctx      context.Context
	repo     *MinioRepository
	template *T
}

func (c TypedQuery[T]) SelectFromTable(table schema.Table) WhereContainer[T] {
	return WhereContainer[T]{c.ctx, c.repo, table, c.template}
}

type WhereContainer[T any] struct {
	ctx      context.Context
	repo     *MinioRepository
	table    schema.Table
	template *T
}

func (w WhereContainer[T]) WhereIndexedFieldEquals(field string, value string) FindByIndexedFieldContainer[T] {
	return FindByIndexedFieldContainer[T]{w.ctx, w.repo, w.table, w.template, field, value}
}

func (w WhereContainer[T]) WhereIdEquals(id string) FindByIdContainer[T] {
	return FindByIdContainer[T]{w.ctx, w.repo, w.table, w.template, id}
}

type FindByIndexedFieldContainer[T any] struct {
	ctx      context.Context
	repo     *MinioRepository
	table    schema.Table
	template *T
	field    string
	value    string
}

// sql: select * from table_name where column1 = value1 (column1 is in an index)
// returns a slice of entities where the foreign key matches
// Param: destination - the address of a slice of T, where the results will be stored
func (f FindByIndexedFieldContainer[T]) Find(destination *[]*T) error {
	coordinates := make([]schema.DatabaseTableIdTuple, 0, 10)
	if err := f.FindIds(&coordinates); err != nil {
		return err
	}

	results := make([]T, len(coordinates))
	errors := make([]*error, len(coordinates))
	var wg sync.WaitGroup
	wg.Add(len(coordinates))
	var mu sync.Mutex

	for i, coordinate := range coordinates {
		go func(i int, coordinate schema.DatabaseTableIdTuple) {
			defer wg.Done()
			path, err := f.table.PathFromIndex(&coordinate)
			if err != nil {
				errors[i] = &err
				return
			}
			// read the actual record
			recordData, err := f.repo.Client.GetObject(f.ctx, f.repo.BucketName, path, minio.GetObjectOptions{})
			if err != nil {
				errors[i] = &err
				return
			}
			defer recordData.Close()

			// parse the actual record
			b, err := io.ReadAll(recordData)
			if err != nil {
				errors[i] = &err
				return
			}
			mu.Lock()
			if err := json.Unmarshal(b, f.template); err != nil {
				errors[i] = &err
				return
			}
			results[i] = *f.template
			mu.Unlock()
		}(i, coordinate)
	}
	wg.Wait()
	for _, err := range errors {
		if err != nil {
			return *err
		}
	}

	*destination = make([]*T, 0, len(results))
	for _, result := range results {
		*destination = append(*destination, &result)
	}
	return nil
}

func (f FindByIndexedFieldContainer[T]) FindIds(destination *[]schema.DatabaseTableIdTuple) error {
	paths, err := f.repo.selectPathsFromTableWhereIndexedFieldEquals(f.ctx, f.table, f.field, f.value)
	if err != nil {
		return err
	}
	*destination = make([]schema.DatabaseTableIdTuple, 0, paths.Len())
	for _, path := range paths.Items() {
		databaseTableIdTuple, err := schema.DatabaseTableIdTupleFromPath(path)
		if err != nil {
			return err
		}
		*destination = append(*destination, *databaseTableIdTuple)
	}
	return nil
}

type FindByIdContainer[T any] struct {
	ctx      context.Context
	repo     *MinioRepository
	table    schema.Table
	template *T
	id       string
}

// sql: select * from table_name where id = value1
// returns the entity with the matching id. If no entity is found, returns a NoSuchKeyError
func (f FindByIdContainer[T]) Find(destination *T) error {
	path := f.table.Path(f.id)
	// read the actual record
	recordData, err := f.repo.Client.GetObject(f.ctx, f.repo.BucketName, path, minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer recordData.Close()

	// parse the actual record
	b, err := io.ReadAll(recordData)
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.StatusCode == http.StatusNotFound {
			return &NoSuchKeyError{Details: fmt.Sprintf("object %s does not exist", path)}
		} else {
			return fmt.Errorf("failed to get object with Id %s from table %s: %w", f.id, f.table.Name, err)
		}
	}
	if err := json.Unmarshal(b, f.template); err != nil {
		return err
	}
	*destination = *f.template
	return nil
}

// sql: insert into table_name (column1, column2, column3, column4) values (value1, value2, value3, value4)
// inserts a new entity into the table. If the entity already exists, returns a DuplicateKeyError
func (r *MinioRepository) InsertIntoTable(ctx context.Context, transaction *schema.Transaction, table schema.Table, entity any) error {
	var err error

	// data file for entity
	transactionStep := schema.TransactionStep{
		Id: uuid.New().String(),
		Type: "insert-data",
		ContentType: "application/json",
		InitialETag: "*",
	}
	transaction.Steps = append(transaction.Steps, &transactionStep)

	transactionStep.Data, err = json.Marshal(entity)
	if err != nil {
		return err
	}

	// use reflection to fetch the value of the Id
	var id string
	id, err = getFieldValueAsString(entity, "Id")
	if err != nil {
		return err
	}

	transactionStep.Path = table.Path(id)

	// handle indices
	// use the path as a tree style index. that way, we simply walk down the tree until we find the key
	for _, index := range table.Indices {
		// use reflection to fetch the value of the field that the index is based on
		value, err := getFieldValueAsString(entity, index.Field)
		if err != nil {
			return err
		}

		transactionStep := schema.TransactionStep{
			Id: uuid.New().String(),
			Type: "insert-index",
			ContentType: "text/plain",
			InitialETag: "*", // fail if the object already exists, since this is an insert not an upsert. if someone beat us to it, that would mean a conflict
			Path: index.Path(value, id),
			Data: []byte{}, // no data for index entries
		}
		transaction.Steps = append(transaction.Steps, &transactionStep)
	}

	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return err
	}

	for _, transactionStep := range transaction.Steps {

		opts := minio.PutObjectOptions{
			ContentType: transactionStep.ContentType,
			// TODO useful for auditing, when versioning is enabled
			// UserMetadata: map[string]string{
			// 	"who, what, when, for auditing purposes, same for updates": id,
			// },
			UserMetadata: map[string]string{
				ROLLBACK_ID: transactionStep.Id,
			},
		}
		if transactionStep.InitialETag == "*" {
			opts.SetMatchETagExcept(transactionStep.InitialETag)
		} else {
			opts.SetMatchETag(transactionStep.InitialETag)
		}
	
		uploadInfo, err := r.Client.PutObject(ctx, r.BucketName, transactionStep.Path, bytes.NewReader(transactionStep.Data), int64(len(transactionStep.Data)), opts)
		if err != nil {
			respErr := minio.ToErrorResponse(err)
			if respErr.StatusCode == http.StatusPreconditionFailed {
				return &DuplicateKeyError{Details: fmt.Sprintf("object %s already exists", transactionStep.Path)}
			} else {
				return fmt.Errorf("failed to put object with Id %s to path %s: %w", transactionStep.Id, transactionStep.Path, err)
			}
		}
		transactionStep.FinalETag = uploadInfo.ETag
		transactionStep.FinalVersionId = uploadInfo.VersionID
	}

	// update the transaction again, now that the ETags are known
	// if this step fails, we can still rollback, because we use the meta data in such cases to find the right version to remove
	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return err
	}

	return nil
}

// sql: select * from table_name where column1 = value1 (column1 is in an index)
func (r *MinioRepository) selectPathsFromTableWhereIndexedFieldEquals(ctx context.Context, table schema.Table, field string, value string) (*util.MutList[string], error) {
	var paths = util.NewMutList[string]()
	// use the index definition to find the path of the actual record containing the data that the caller wants to read
	index, err := table.GetIndex(field)
	if err != nil {
		return paths, err
	}
	indexPath := index.PathNoId(value) + "/" // add a slash since we don't do a recursive search, and without it, it just returns the folder, not the files in the folder

	// Channel to hold object names to be removed
	objectsCh := make(chan minio.ObjectInfo)
	errors := util.NewMutList[error]()

	// Goroutine to list objects and send them to the channel
	go func() {
		defer close(objectsCh) // Close the channel when listing is done
		listOpts := minio.ListObjectsOptions{
			Prefix: indexPath, // default is non-recursive
		}
		for object := range r.Client.ListObjects(ctx, r.BucketName, listOpts) {
			if object.Err != nil {
				errors.Add(object.Err)
			} else {
				objectsCh <- object
			}
		}
	}() // End of goroutine

	// Process objects from the channel
	for object := range objectsCh {
		if object.Err != nil {
			errors.Add(object.Err)
		} else {
			paths.Add(object.Key)
		}
	}

	if errors.Len() > 0 {
		return paths, errors.Items()[0]
	}

	return paths, nil
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

func ReadObjectVersionOlderThanTimestamp(ctx context.Context, table schema.Table, id string, timestamp time.Time) (*[]byte, error) {
	objects := repo.Client.ListObjects(ctx, repo.BucketName, minio.ListObjectsOptions{
		Prefix:       table.Path(id), // full path of object we are reading
		WithVersions: true,           // get version info so that we can find the object with a timestamp before the transaction started
	})

	// reverse sort
	// TODO no need to do this, since we can set ReverseVersions to true in the list options
	var objectsList []minio.ObjectInfo
	for object := range objects {
		if object.Err != nil {
			return nil, object.Err
		}
		objectsList = append(objectsList, object)
	}
	slices.SortFunc(objectsList, func(a, b minio.ObjectInfo) int {
		return b.LastModified.Compare(a.LastModified) // oldest first
	})

	versionToRead := ""
	for _, object := range objectsList {
		if object.Err != nil {
			return nil, object.Err
		}

		if object.LastModified.Before(timestamp) {
			versionToRead = object.VersionID
			break
		}
	}

	if versionToRead == "" {
		return nil, &NoSuchKeyError{Details: fmt.Sprintf("object %s does not exist", table.Path(id))}
	}

	// read the object
	objectData, err := repo.Client.GetObject(ctx, repo.BucketName, table.Path(id), minio.GetObjectOptions{
		VersionID: versionToRead,
	})
	if err != nil {
		return nil, err
	}
	defer objectData.Close()

	// parse the actual record
	b, err := io.ReadAll(objectData)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (r *MinioRepository) StartTransaction(ctx context.Context, timeout time.Duration) (schema.Transaction, error) {
	tx := schema.NewTransaction(timeout)
	err := r.updateTransaction(ctx, &tx)
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.StatusCode == http.StatusPreconditionFailed {
			return tx, &DuplicateKeyError{Details: fmt.Sprintf("transaction %s already exists", tx.Id)}
		} else {
			return tx, fmt.Errorf("failed to put transaction %s: %w", tx.Id, err)
		}
	}
	return tx, nil
}

func (r *MinioRepository) updateTransaction(ctx context.Context, transaction *schema.Transaction) error {
	json, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	opts := minio.PutObjectOptions{
		ContentType: "application/json",

	}
	if transaction.Etag == "*" {
		opts.SetMatchETagExcept(transaction.Etag)
	} else {
		opts.SetMatchETag(transaction.Etag)
	}
	uploadInfo, err := r.Client.PutObject(ctx, r.BucketName, transaction.GetPath()+"/tx.json", bytes.NewReader(json), int64(len(json)), opts)
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.StatusCode == http.StatusPreconditionFailed {
			return &DuplicateKeyError{Details: fmt.Sprintf("transaction %s already exists", transaction.Id)}
		} else {
			return fmt.Errorf("failed to put transaction %s: %w", transaction.Id, err)
		}
	}
	transaction.Etag = uploadInfo.ETag
	return nil
}

func (r *MinioRepository) GetOpenTransactions(ctx context.Context, transactions *[]schema.Transaction) error {
	// read all objects in the transactions folder
	objectCh := r.Client.ListObjects(ctx, r.BucketName, minio.ListObjectsOptions{
		Prefix:    "transactions/",
		Recursive: false,
	})
	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}

		// read the actual tx
		txData, err := r.Client.GetObject(ctx, r.BucketName, object.Key+"tx.json", minio.GetObjectOptions{})
		if err != nil {
			return err
		}
		defer txData.Close()

		// parse the actual record
		b, err := io.ReadAll(txData)
		if err != nil {
			return err
		}
		var transaction = &schema.Transaction{}
		if err := json.Unmarshal(b, &transaction); err != nil {
			return err
		}
		*transactions = append(*transactions, *transaction)
	}
	return nil
}

func (r *MinioRepository) Commit(ctx context.Context, tx *schema.Transaction) error {
	// delete the transaction
	governanceBypass := true // transactions are not subject to governance
	if err := r.DeleteFolder(ctx, tx.GetPath(), governanceBypass, true); err != nil {
		return err
	}
	return nil
}

func (r *MinioRepository) Rollback(ctx context.Context, tx *schema.Transaction) []error {
	errs := make([]error, 0, 10) // remove as much as possible
	// go through each transaction step in reverse order and delete exactly that version
	for i := len(tx.Steps) - 1; i >= 0; i-- {
		step := tx.Steps[i]

		versionIds := make([]string, 0, 10) // ok, there really should only ever be one version, but let's be paranoid in the case that we were unable to update the transaction after putting objects
		if step.FinalVersionId != "" {
			versionIds = append(versionIds, step.FinalVersionId)
		}

		if len(versionIds) == 0 {
			for object := range r.Client.ListObjects(ctx, r.BucketName, minio.ListObjectsOptions{
				Prefix: step.Path,
				WithVersions: true,
				WithMetadata: true,
			}) {
				if object.Err != nil {
					errs = append(errs, object.Err)
				} else {
					// delete it, if the metadata matches
					// not working: var metaDataRollbackId string = object.UserMetadata[ROLLBACK_ID]
					var metaDataRollbackId string = object.UserMetadata["X-Amz-Meta-Rollbackid"]
					if metaDataRollbackId == step.Id {
						versionIds = append(versionIds, object.VersionID)
					}
				}
			}
		}

		for _, versionId := range versionIds {
			err := r.Client.RemoveObject(ctx, r.BucketName, step.Path, minio.RemoveObjectOptions{
				VersionID: versionId,
			})
			if err != nil {
				errs = append(errs, err)
			}
		}

		// TODO do we need to look at the step.Type?
		// TODO how do we deal with deletion, i.e. make the object visible again
	}

	if len(errs) == 0 {
		governanceBypass := true // transactions are not subject to governance
		err := r.DeleteFolder(ctx, tx.GetPath(), governanceBypass, true)
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

func (r *MinioRepository) DeleteFolder(ctx context.Context, folderPrefix string, governanceBypass bool, deleteAllVersions bool) error {
	// Ensure folderPrefix ends with a slash for proper folder deletion
	if !strings.HasSuffix(folderPrefix, "/") {
		folderPrefix = folderPrefix + "/"
	}

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
			WithVersions: deleteAllVersions, // to get all versions of the objects and delete everything
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
		GovernanceBypass: governanceBypass,
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
		errorString := ""
		for _, e := range errors {
			errorString += e.Err.Error() + "\n"
		}
		return fmt.Errorf("Finished deletion process with %d errors. All errors were:\n%s", len(errors), errorString)
	}
	return nil
}
