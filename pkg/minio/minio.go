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
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const MINIO_META_PREFIX = "X-Amz-Meta-"

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

type TypedQuery[T any] struct {
	ctx      context.Context
	template *T
	repo     *MinioRepository
	tx       *schema.Transaction
}

// Param: repo - the repository to use
// Param: ctx - the context to use
// Param: transaction - the transaction to use
// Param: template - the template of type T, used to unmarshal the JSON data
// Returns: a new TypedQuery[T] instance
func NewTypedQuery[T any](repo *MinioRepository, ctx context.Context, transaction *schema.Transaction, template *T) TypedQuery[T] {
	return TypedQuery[T]{
		ctx:      ctx,
		template: template,
		repo:     repo,
		tx:       transaction,
	}
}

type ContextContainer[T any] struct {
	ctx      context.Context
	repo     *MinioRepository
	template *T
	tx       *schema.Transaction
}

func (c TypedQuery[T]) SelectFromTable(table schema.Table) WhereContainer[T] {
	return WhereContainer[T]{c.ctx, c.repo, table, c.template, c.tx}
}

type WhereContainer[T any] struct {
	ctx      context.Context
	repo     *MinioRepository
	table    schema.Table
	template *T
	tx       *schema.Transaction
}

func (w WhereContainer[T]) WhereIndexedFieldEquals(field string, value string) FindByIndexedFieldContainer[T] {
	return FindByIndexedFieldContainer[T]{w.ctx, w.repo, w.table, w.template, field, value, w.tx}
}

func (w WhereContainer[T]) WhereIdEquals(id string) FindByIdContainer[T] {
	return FindByIdContainer[T]{w.ctx, w.repo, w.table, w.template, id, w.tx}
}

type FindByIndexedFieldContainer[T any] struct {
	ctx      context.Context
	repo     *MinioRepository
	table    schema.Table
	template *T
	field    string
	value    string
	tx       *schema.Transaction
}

// sql: select * from table_name where column1 = value1 (column1 is in an index)
// returns a slice of entities where the foreign key matches
// Param: destination - the address of a slice of T, where the results will be stored
func (f FindByIndexedFieldContainer[T]) Find(destination *[]*T) error {
	coordinates := make([]schema.DatabaseTableIdTuple, 0, 10)
	if err := f.FindIds(&coordinates); err != nil {
		return err
	}

	results := make([]*T, len(coordinates))
	errors := make([]*error, len(coordinates))
	var wg sync.WaitGroup
	wg.Add(len(coordinates))
	var mu sync.Mutex

	// get in parallel
	for i, coordinate := range coordinates {
		go func(i int, coordinate schema.DatabaseTableIdTuple) {
			defer wg.Done()
			path, err := f.table.PathFromIndex(&coordinate)
			if err != nil {
				errors[i] = &err
				return
			}

			err = getById(f.ctx, f.repo, f.tx, path, f.template)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errors[i] = &err
			} else {
				results[i] = f.template
			}
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
		*destination = append(*destination, result)
	}
	return nil
}

func (f FindByIndexedFieldContainer[T]) FindIds(destination *[]schema.DatabaseTableIdTuple) error {
	paths, err := f.repo.selectPathsFromTableWhereIndexedFieldEquals(f.ctx, f.tx, f.table, f.field, f.value)
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
	tx       *schema.Transaction
}

// sql: select * from table_name where id = value1
// returns the entity with the matching id. If no entity is found, returns a NoSuchKeyError
func (f FindByIdContainer[T]) Find(destination *T) error {
	path := f.table.Path(f.id)

	return getById(f.ctx, f.repo, f.tx, path, destination)

}

func getById[T any](ctx context.Context, repo *MinioRepository, transaction *schema.Transaction, path string, destination *T) error {
	if err := transaction.IsOk(); err != nil {
		return err
	}
	if cached, ok := transaction.Cache[path]; ok {
		val := *cached
		var t *T = val.(*T)
		*destination = *t
	} else {
		found := false
		// do not load versions older than the start of the transaction (part of the mvcc principle)
		for object := range repo.Client.ListObjects(ctx, repo.BucketName, minio.ListObjectsOptions{
			Prefix: path,
			WithVersions: true,
			ReverseVersions: true,
			WithMetadata: true,
		}) {
			if object.Err != nil {
				return object.Err
			}

			if object.LastModified.UnixMicro() < transaction.StartMicroseconds {
				// read the actual record
				recordData, err := repo.Client.GetObject(ctx, repo.BucketName, path, minio.GetObjectOptions{})
				if err != nil {
					return err
				}
				defer recordData.Close()
			
				// parse the actual record
				b, err := io.ReadAll(recordData)
				if err != nil {
					respErr := minio.ToErrorResponse(err)
					if respErr.StatusCode == http.StatusNotFound {
						return &NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", path)}
					} else {
						return fmt.Errorf("failed to get object with Path %s: %w", path, err)
					}
				}
				if err := json.Unmarshal(b, destination); err != nil {
					return err
				}
				found = true
				break
			}	
		}	

		if !found {
			return &NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", path)}
		} else {
			// cache the result in case it is read again
			var a any = destination
			transaction.Cache[path] = &a
		}
	}
	return nil
}

// sql: insert into table_name (column1, column2, column3, column4) values (value1, value2, value3, value4)
// inserts a new entity into the table. If the entity already exists, returns a DuplicateKeyError
func (r *MinioRepository) InsertIntoTable(ctx context.Context, transaction *schema.Transaction, table schema.Table, entity any) error {
	var err error

	// //////////////////////////////////////////////////
	// handle object
	// //////////////////////////////////////////////////

	// use reflection to fetch the value of the Id
	var id string
	id, err = getFieldValueAsString(entity, "Id")
	if err != nil {
		return err
	}

	err = transaction.AddStep("insert-data", "application/json", table.Path(id), "*", entity)
	if err != nil {
		return err
	}

	// //////////////////////////////////////////////////
	// handle indices
	// //////////////////////////////////////////////////
	// use the path as a tree style index. that way, we simply walk down the tree until we find the key
	var indexPathsBuilder strings.Builder
	for _, index := range table.Indices {
		// use reflection to fetch the value of the field that the index is based on
		value, err := getFieldValueAsString(entity, index.Field)
		if err != nil {
			return err
		}

		 // ETag: "*" - fail if the object already exists, since this is an insert not an upsert. if someone beat us to it, that would mean a conflict
		err = transaction.AddStep("insert-index", "text/plain", index.Path(value, id), "*", nil)
		if err != nil {
			return err
		}
		indexPathsBuilder.WriteString(index.Path(value, id))
		indexPathsBuilder.WriteByte('\n')
	}

	// //////////////////////////////////////////////////
	// store the indices for this object, so that if we
	// update or delete it, we know what to replace
	// //////////////////////////////////////////////////
	indicesPath := table.IndicesPath(id)
	indicesData := []byte(indexPathsBuilder.String())
	err = transaction.AddStep("insert-reverse-indices", "text/plain", indicesPath, "*", indicesData)
	if err != nil {
		return err
	}

	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return err
	}

	// //////////////////////////////////////////////////
	// execute the transaction steps
	// //////////////////////////////////////////////////
	err = executeTransactionSteps(ctx, *r.Client, r.BucketName, transaction, id)
	if err != nil {
		return err
	}

	// //////////////////////////////////////////////////
	// update the transaction again, now that the ETags are known
	// //////////////////////////////////////////////////
	// if this step fails, we can still rollback, because we use the meta data in such cases to find the right version to remove
	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return err
	}

	return nil
}

// sql: update table_name set everything where id = ?
// does an optimistically locked update on an entity.
// If the ETag is wrong this method returns a StaleObjectError.
// If the object doesn't exist this method returns a NoSuchKeyError
func (r *MinioRepository) UpdateTable(ctx context.Context, transaction *schema.Transaction, table schema.Table, entity any, etag string) error {
	var err error

	// //////////////////////////////////////////////////
	// handle object
	// //////////////////////////////////////////////////

	// use reflection to fetch the value of the Id
	var id string
	id, err = getFieldValueAsString(entity, "Id")
	if err != nil {
		return err
	}

	err = transaction.AddStep("update-data", "application/json", table.Path(id), etag, entity)
	if err != nil {
		return err
	}

	// //////////////////////////////////////////////////
	// read existing indices to decide which index files
	// to keep
	// //////////////////////////////////////////////////
	object, err := r.Client.GetObject(ctx, r.BucketName, table.IndicesPath(id), minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer object.Close()
	b, err := io.ReadAll(object)
	if err != nil {
		return err
	}
	existingIndices := strings.Split(string(b), "\n")

	// //////////////////////////////////////////////////
	// handle new indices
	// //////////////////////////////////////////////////
	newIndices := make([]string, 0, len(table.Indices))
	for _, index := range table.Indices {
		// use reflection to fetch the value of the field that the index is based on
		value, err := getFieldValueAsString(entity, index.Field)
		if err != nil {
			return err
		}

		indexPath := index.Path(value, id)
		if !slices.Contains(existingIndices, indexPath) {
			// ETag: "" - we need to overwrite
			err = transaction.AddStep("update-insert-index", "text/plain", indexPath, "", nil)
			if err != nil {
				return err
			}
		} // else keep it

		newIndices = append(newIndices, indexPath)
	}

	// //////////////////////////////////////////////////
	// calculate which ones to delete
	// //////////////////////////////////////////////////
	for _, existingIndex := range existingIndices {
		if !slices.Contains(newIndices, existingIndex) {
			// ETag: "" - not relevant for deletion
			err = transaction.AddStep("update-delete-index", "text/plain", existingIndex, "", nil)
			if err != nil {
				return err
			}
		}
	}

	// //////////////////////////////////////////////////
	// handle reverse indices
	// //////////////////////////////////////////////////
	// use the path as a tree style index. that way, we simply walk down the tree until we find the key
	indicesPath := table.IndicesPath(id)
	indicesData := []byte(strings.Join(newIndices, "\n"))
	err = transaction.AddStep("update-reverse-indices", "text/plain", indicesPath, "", indicesData) // Etag: "" - we need to overwrite the file
	if err != nil {
		return err
	}

	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return err
	}

	// //////////////////////////////////////////////////
	// execute the transaction steps
	// //////////////////////////////////////////////////
	err = executeTransactionSteps(ctx, *r.Client, r.BucketName, transaction, id)
	if err != nil {
		return err
	}

	// update the transaction again, now that the ETags are known
	// if this step fails, we can still rollback, because we use the meta data in such cases to find the right version to remove
	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return err
	}

	return nil
}

func executeTransactionSteps(ctx context.Context, client minio.Client, bucketName string, transaction *schema.Transaction, id string) error {
	for _, transactionStep := range transaction.Steps {
		if transactionStep.Executed {
			continue
		}

		opts := minio.PutObjectOptions{
			ContentType: transactionStep.ContentType,
			// TODO useful for auditing, when versioning is enabled
			// UserMetadata: map[string]string{
			// 	"who, what, when, for auditing purposes, same for updates": id,
			// },
			UserMetadata: transactionStep.UserMetadata,
		}
		if transactionStep.InitialETag == "" {
			// ignore, since the caller wants to overwrite in all cases
		} else if transactionStep.InitialETag == "*" {
			opts.SetMatchETagExcept(transactionStep.InitialETag)
		} else {
			opts.SetMatchETag(transactionStep.InitialETag)
		}
	
		if strings.Contains(transactionStep.Type, "delete") {
			opts := minio.RemoveObjectOptions{
			}
			if transactionStep.InitialVersionId != "" {
				opts.VersionID = transactionStep.InitialVersionId
			}
			err := client.RemoveObject(ctx, bucketName, transactionStep.Path, opts)
			if err != nil {
				return err
			}
		} else { // insert/update
			uploadInfo, err := client.PutObject(ctx, bucketName, transactionStep.Path, bytes.NewReader(*transactionStep.Data), int64(len(*transactionStep.Data)), opts)
			if err != nil {
				respErr := minio.ToErrorResponse(err)
				if respErr.StatusCode == http.StatusPreconditionFailed {
					return &DuplicateKeyErrorWithDetails{Details: fmt.Sprintf("object %s already exists", transactionStep.Path)}
				} else {
					return fmt.Errorf("failed to put object with Id %s to path %s: %w", id, transactionStep.Path, err)
				}
			}
	
			transactionStep.FinalETag = uploadInfo.ETag
			transactionStep.FinalVersionId = uploadInfo.VersionID
			transactionStep.Executed = true
		}
	}

	return nil
}

// sql: select * from table_name where column1 = value1 (column1 is in an index)
func (r *MinioRepository) selectPathsFromTableWhereIndexedFieldEquals(ctx context.Context, transaction *schema.Transaction, table schema.Table, field string, value string) (*util.MutList[string], error) {
	matchingPaths := util.NewMutList[string]()
	// use the index definition to find the path of the actual record containing the data that the caller wants to read
	index, err := table.GetIndex(field)
	if err != nil {
		return matchingPaths, err
	}
	indexPath := index.PathNoId(value) + "/" // add a slash since we don't do a recursive search, and without it, it just returns the folder, not the files in the folder

	// only need one per path. but there can be multiple version, so only select the one that is older than the transaction start time
	relevantPaths := make(map[string]bool) // effectively a set

	errors := util.NewMutList[error]()

	// Goroutine to list objects and send them to the channel
	listOpts := minio.ListObjectsOptions{
		Prefix: indexPath, // default is non-recursive
		WithMetadata: true,
		// versions are irrelevant on index entries because we store no data, just the path. so we use the metadata to know if it was created after the tx started (e.g. by a different transaction)
	}
	for object := range r.Client.ListObjects(ctx, r.BucketName, listOpts) {
		if object.Err != nil {
			errors.Add(object.Err)
		} else {
			lastModifiedFromMetadata := object.UserMetadata[MINIO_META_PREFIX+schema.LAST_MODIFIED]
			lastModified, err := strconv.ParseInt(lastModifiedFromMetadata, 10, 64)
			if err != nil {
				errors.Add(err)
			}
			if lastModified < transaction.StartMicroseconds {
				relevantPaths[object.Key] = true
			}
		}
	}

	// TODO cope with deleted index entries?

	// add anything from the cache that matches the path, because those have a LastModified in Minio that is newer than the tx start, but they are still relevant
	for key := range transaction.Cache {
		if strings.HasPrefix(key, indexPath) {
			relevantPaths[key] = true
		}
	}

	for key := range relevantPaths {
		matchingPaths.Add(key)
	}

	if errors.Len() > 0 {
		return matchingPaths, errors.Items()[0]
	}

	return matchingPaths, nil
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
		return nil, &NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", table.Path(id))}
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
			return tx, &DuplicateKeyErrorWithDetails{Details: fmt.Sprintf("transaction %s already exists", tx.Id)}
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
			return &DuplicateKeyErrorWithDetails{Details: fmt.Sprintf("transaction %s already exists", transaction.Id)}
		} else {
			return fmt.Errorf("failed to put transaction %s: %w", transaction.Id, err)
		}
	}
	transaction.Etag = uploadInfo.ETag
	return nil
}

func (r *MinioRepository) GetTransactionsInProgress(ctx context.Context, transactions *[]schema.Transaction) error {
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
	tx.State = "Committed"
	err := r.updateTransaction(ctx, tx) // store in case this process fails and needs recovering
	if err != nil {
		return err
	}

	// delete the transaction
	governanceBypass := true // transactions are not subject to governance
	if err := r.DeleteFolder(ctx, tx.GetPath(), governanceBypass, true); err != nil {
		return err
	}
	return nil
}

func (r *MinioRepository) Rollback(ctx context.Context, tx *schema.Transaction) []error {
	tx.State = "RolledBack"
	err := r.updateTransaction(ctx, tx) // store in case this process fails and needs recovering
	if err != nil {
		return []error{err}
	}

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
					var metaDataRollbackId string = object.UserMetadata[MINIO_META_PREFIX+schema.ROLLBACK_ID]
					if metaDataRollbackId == step.UserMetadata[schema.ROLLBACK_ID] {
						versionIds = append(versionIds, object.VersionID)
					}
				}
			}
		}

		var wg sync.WaitGroup
		wg.Add(len(versionIds))
		var mu sync.Mutex
		for _, versionId := range versionIds {
			go func(versionId string) {
				defer wg.Done()
				err := r.Client.RemoveObject(ctx, r.BucketName, step.Path, minio.RemoveObjectOptions{
					VersionID: versionId,
				})
				if err != nil {
					mu.Lock()
					defer mu.Unlock()
					errs = append(errs, err)
				}
			}(versionId)
		}
		wg.Wait()

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
