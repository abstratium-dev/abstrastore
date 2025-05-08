package minio

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
const TX_FILENAME = "tx.json"
const TOMBSTONE_AND_EXISTS_UNTIL = "Tombstone-And-Exists-Until" // wow, minio doesn't support camel case
const MAX_TX_TIMEOUT_MICROS = 10 * 60 * 1000 * 1000 // 10 minutes
const GC_ROOT = "gc/"

var repo *MinioRepository
var theCallback Callback

type MinioRepository struct {
	Client     *minio.Client
	BucketName string

	// no need for any locks - see https://github.com/minio/minio-go/issues/1125, which seems to have fixed any issues related to goroutine-safety
}

func Setup(callback Callback) {
	theCallback = callback
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

	// add a timer which runs every 10 seconds to delete any files in the GC folder
	go func() {
		for {
			ExecuteGc()
			time.Sleep(10 * time.Second)
		}
	}()
}

func ExecuteGc() {
	for objectInfo := range repo.Client.ListObjects(context.Background(), repo.BucketName, minio.ListObjectsOptions{
		Prefix: GC_ROOT,
		WithVersions: true,
		WithMetadata: true,
	}) {
		if objectInfo.Err != nil {
			theCallback.ErrorDuringGc(objectInfo.Err)
		} else {
			// note that if two GC entries were created in the same microsecond, there could be two versions of the
			// object, and we need to delete both
			key := objectInfo.Key[len(GC_ROOT):]
			keepUntil, err := strconv.ParseInt(key, 10, 64)
			if err != nil {
				theCallback.ErrorDuringGc(fmt.Errorf("ADB-0017 failed to parse GC entry %s: %w", objectInfo.Key, err))
				continue
			}
			
			now := time.Now().UnixMicro()
			if now > keepUntil {
				// read the contents as a string which is the path of the file that actually needs deleting
				object, err := repo.Client.GetObject(context.Background(), repo.BucketName, objectInfo.Key, minio.GetObjectOptions{
					VersionID: objectInfo.VersionID,
				})
				if err != nil {
					theCallback.ErrorDuringGc(fmt.Errorf("ADB-0029 failed to get object %s: %w", objectInfo.Key, err))
				}
				defer object.Close()
				fileDoesNotExist := false

				// read the contents as a string
				contents, err := io.ReadAll(object)
				if err != nil {
					respErr := minio.ToErrorResponse(err)
					if respErr.StatusCode == http.StatusNotFound {
						// if the error is "not found", then ignore it, another pod seems to have handled it already,
						// between this one listing the objects, and then reading the actual object
						fileDoesNotExist = true
					} else {
						theCallback.ErrorDuringGc(fmt.Errorf("ADB-0018 failed to read contents of gc entry %s: %w", objectInfo.Key, err))
					}
				}

				if !fileDoesNotExist {
					pathToDelete := string(contents)
	
					// now remove the actual file that was marked for deletion.
					// this method doesn't seem to throw an error if the object doesn't exist, perhaps since we aren't setting a version id.
					// it could be missing if another pod is running in parallel.
					// but fine, if it isn't found, and no error comes, that works for me.
					err = repo.Client.RemoveObject(context.Background(), repo.BucketName, pathToDelete, minio.RemoveObjectOptions{})
					if err != nil {
						theCallback.ErrorDuringGc(fmt.Errorf("ADB-0031 failed to remove file %s referenced in gc entry %s: %w", pathToDelete, objectInfo.Key, err))
					}
				}	

				// now remove the gc entry, as we have cleaned up everything
				err = repo.Client.RemoveObject(context.Background(), repo.BucketName, objectInfo.Key, minio.RemoveObjectOptions{
					VersionID: objectInfo.VersionID,
				})
				if err != nil {
					theCallback.ErrorDuringGc(fmt.Errorf("ADB-0030 failed to remove gc entry %s: %w", objectInfo.Key, err))
				}
			}
		}
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

func (w WhereContainer[T]) WhereIndexedFieldEquals(fieldName string, value string) FindByIndexedFieldContainer[T] {
	return FindByIndexedFieldContainer[T]{w.ctx, w.repo, w.table, w.template, fieldName, value, w.tx}
}

func (w WhereContainer[T]) WhereIdEquals(id string) FindByIdContainer[T] {
	return FindByIdContainer[T]{w.ctx, w.repo, w.table, w.template, id, w.tx}
}

type FindByIndexedFieldContainer[T any] struct {
	ctx      context.Context
	repo     *MinioRepository
	table    schema.Table
	template *T
	fieldName    string
	value    string
	tx       *schema.Transaction
}

// sql: select * from table_name where column1 = value1 (column1 is in an index)
// Param: destination - the address of a slice of T, where the results will be stored, i.e. a slice of entities where the foreign key matches
// Returns: a map of entity ids to ETags, and an error if any occurred
func (f FindByIndexedFieldContainer[T]) Find(destination *[]*T) (*map[string]*string, error) {
	var etags = make(map[string]*string)
	coordinates := make([]schema.DatabaseTableIdTuple, 0, 10)
	if err := f.findIds(&coordinates); err != nil {
		return nil, err
	}

	// the coordinates should all belong to the same DB and table, otherwise something is odd with the indices
	if len(coordinates) > 0 {
		containsDifferingDbOrTable := slices.ContainsFunc(coordinates, func(coordinate schema.DatabaseTableIdTuple) bool {
			return coordinate.Database != coordinates[0].Database || coordinate.Table != coordinates[0].Table
		})
		if containsDifferingDbOrTable {
			return nil, fmt.Errorf("ADB-0011: all coordinates should belong to the same DB and table, but they don't: %v", coordinates)
		}
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

			var etag *string
			etag, existsInTx, err := getByPath(f.ctx, f.repo, f.tx, path, f.template)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errors[i] = &err
			} else if existsInTx {
				results[i] = f.template
				etags[coordinate.Id] = etag
			} else {
				results[i] = nil
				etags[coordinate.Id] = nil
			}
		}(i, coordinate)
	}
	wg.Wait()
	for _, err := range errors {
		if err != nil {
			return nil, *err
		}
	}

	*destination = make([]*T, 0, len(results))
	for _, result := range results {
		if result == nil {
			// happens after a delete that is not yet committed
			continue
		}
		// need to double check that the object that was loaded actually really matches the predicate.
		// that is because we cannot fully trust the indices - they still contain old entries in case other transactions
		// need to find old versions of data, and they contain new entries, which while those should have been skipped
		// if they belong to transactions that are in progress, it is still better to double check here.
		fieldValue, err := getFieldValueAsString(result, f.fieldName)
		if err != nil {
			return nil, err
		}
		if fieldValue == f.value { // TODO other predicates too like regex, <, >, etc.
			*destination = append(*destination, result)
		}
	}
	return &etags, nil
}

// not public, because without checking metadata of actual files, against transactions in progress, it's not safe to use these.
// we pass these up, but the caller must ensure that versions exist for this transaction by comparing to others that are in progress
func (f FindByIndexedFieldContainer[T]) findIds(destination *[]schema.DatabaseTableIdTuple) error {
	paths, err := f.repo.selectPathsFromTableWhereIndexedFieldEquals(f.ctx, f.tx, f.table, f.fieldName, f.value)
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
func (f FindByIdContainer[T]) Find(destination *T) (*string, error) {
	path := f.table.Path(f.id)

	etag, existsInTx, err := getByPath(f.ctx, f.repo, f.tx, path, destination)
	if err != nil {
		return nil, err
	}
	if !existsInTx {
		return nil, &NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", path)}
	}
	return etag, nil
}

// returns the ETag of the object, whether it exists in the transaction (false if the tx cache is explicitly nil), and an error if any occurred
func getByPath[T any](ctx context.Context, repo *MinioRepository, transaction *schema.Transaction, path string, destination *T) (*string, bool, error) {
	var etag *string
	if err := transaction.IsOk(); err != nil {
		return nil, false, err
	}
	if cached, ok := transaction.Cache[path]; ok {
		if cached == nil {
			return nil, false, nil
		} else {
			etag = cached.ETag
			val := *cached.Object
			var t *T = val.(*T)
			*destination = *t
		}
	} else {
		var objectData *[]byte
		var err error
		objectData, etag, err = repo.readObjectVersionForTransaction(ctx, transaction, path)
		if err != nil {
			return nil, false, err
		}
		if objectData != nil {
			if len(*objectData) == 0 {
				// it has been deleted in the version that was found
				transaction.Cache[path] = nil
				return nil, false, &NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", path)}
			}
			if err := json.Unmarshal(*objectData, destination); err != nil {
				return nil, false, err
			}
			// cache the result in case it is read again
			var a any = destination
			transaction.Cache[path] = &schema.ObjectAndETag{Object: &a, ETag: etag}
		} else {
			return nil, false, &NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", path)}
		}
	}
	return etag, true, nil
}

// sql: insert into table_name (column1, column2, column3, column4) values (value1, value2, value3, value4)
// inserts a new entity into the table.
// If the entity already exists, returns a DuplicateKeyError.
// If the entity is about to be written by a different transaction, returns a ObjectLockedError.
func (r *MinioRepository) InsertIntoTable(ctx context.Context, transaction *schema.Transaction, table schema.Table, entity any) (*string, error) {

	if err := transaction.IsOk(); err != nil {
		return nil, err
	}

	var err error

	// //////////////////////////////////////////////////
	// handle object
	// //////////////////////////////////////////////////

	// use reflection to fetch the value of the Id
	var id string
	id, err = getFieldValueAsString(entity, "Id")
	if err != nil {
		return nil, err
	}

	err = transaction.AddStep("insert-data", "application/json", table.Path(id), "*", &entity)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		 // ETag: "*" - fail if the object already exists, since this is an insert not an upsert. if someone beat us to it, that would mean a conflict
		err = transaction.AddStep("insert-add-index", "text/plain", index.Path(value, id), "*", nil)
		if err != nil {
			return nil, err
		}
		indexPathsBuilder.WriteString(index.Path(value, id))
		indexPathsBuilder.WriteByte('\n')
	}

	// //////////////////////////////////////////////////
	// store the indices for this object, so that if we
	// update or delete it, we know what to replace
	// //////////////////////////////////////////////////
	indicesPath := table.IndicesPath(id)
	var indicesAsString any = indexPathsBuilder.String()
	err = transaction.AddStep("insert-reverse-indices", "text/plain", indicesPath, "*", &indicesAsString)
	if err != nil {
		return nil, err
	}

	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return nil, err
	}

	// //////////////////////////////////////////////////
	// execute the transaction steps
	// //////////////////////////////////////////////////
	var etag *string
	etag, err = r.executeTransactionSteps(ctx, transaction, id, 0)
	if err != nil {
		return nil, err
	}

	// //////////////////////////////////////////////////
	// update the transaction again, now that the ETags are known
	// //////////////////////////////////////////////////
	// if this step fails, we can still rollback, because we use the meta data in such cases to find the right version to remove
	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return nil, err
	}

	return etag, nil
}

// sql: update table_name set everything where id = ?
// does an optimistically locked update on an entity.
// If the ETag is wrong this method returns a StaleObjectError.
// If the ETag is '*', it would be interpreted as meaning that no prior version may exist, i.e. an insert. Please call `insert` instead of `update` in this case.
// If the ETag is an empty string we overwrite in all cases, whether a previous version exists or not. equivalent to "upsert"
// If the object doesn't exist this method returns a NoSuchKeyError.
func (r *MinioRepository) UpdateTable(ctx context.Context, transaction *schema.Transaction, table schema.Table, entity any, etag *string) (*string, error) {

	if err := transaction.IsOk(); err != nil {
		return nil, err
	}

	if *etag == "*" {
		return nil, fmt.Errorf("ADB0031 ETag is '*', which is not allowed for update, use insert instead.")
	}

	var err error

	// //////////////////////////////////////////////////
	// handle object
	// //////////////////////////////////////////////////

	// use reflection to fetch the value of the Id
	var id string
	id, err = getFieldValueAsString(entity, "Id")
	if err != nil {
		return nil, err
	}

	err = transaction.AddStep("update-data", "application/json", table.Path(id), *etag, &entity)
	if err != nil {
		return nil, err
	}

	// //////////////////////////////////////////////////
	// read existing indices to decide which index files
	// to keep
	// //////////////////////////////////////////////////
	object, err := r.Client.GetObject(ctx, r.BucketName, table.IndicesPath(id), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	b, err := io.ReadAll(object)
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.StatusCode == http.StatusNotFound && *etag == "" {
			// OK - the user is trying to do an upsert, and the object doesn't exist, so it has no indices
			b = []byte("\"\"") // empty json string
		} else {
			return nil, err
		}
	}
	// it is a json string => convert back to a normal one
	var existingIndicesAsString string
	err = json.Unmarshal(b, &existingIndicesAsString)
	if err != nil {
		return nil, err
	}
	existingIndices := strings.Split(strings.TrimSpace(existingIndicesAsString), "\n")

	// //////////////////////////////////////////////////
	// add any indices that don't exist yet.
	// at the same time note *all* indices that should 
	// exist, for the reverse index file
	// //////////////////////////////////////////////////
	allIndicesRequiredAfterCommit := make([]string, 0, len(table.Indices))
	for _, index := range table.Indices {
		// use reflection to fetch the value of the field that the index is based on
		value, err := getFieldValueAsString(entity, index.Field)
		if err != nil {
			return nil, err
		}

		indexPath := index.Path(value, id)
		if !slices.Contains(existingIndices, indexPath) {
			// ETag: "" - we need to overwrite
			err = transaction.AddStep("update-add-index", "text/plain", indexPath, "", nil)
			if err != nil {
				return nil, err
			}
		} // else keep it

		allIndicesRequiredAfterCommit = append(allIndicesRequiredAfterCommit, indexPath)
	}

	// //////////////////////////////////////////////////
	// calculate which ones to delete
	// //////////////////////////////////////////////////
	for _, existingIndex := range existingIndices {
		if !slices.Contains(allIndicesRequiredAfterCommit, existingIndex) {
			if existingIndex != "" { // happens on upsert with no prior version (or maybe also when no fields are indexed?)
				// ETag: "" - not relevant for deletion
				err = transaction.AddStep("update-remove-index", "text/plain", existingIndex, "", nil)
				if err != nil {
					return nil, err
				}
			}
		}
	}

	// //////////////////////////////////////////////////
	// handle reverse indices
	// //////////////////////////////////////////////////
	// store all indices as they should be after the tx commits,
	// so that future transactions can know what files to delete,
	// just like this algorithm calculates above.
	indicesPath := table.IndicesPath(id)
	var indicesData any = strings.Join(allIndicesRequiredAfterCommit, "\n")
	err = transaction.AddStep("update-reverse-indices", "text/plain", indicesPath, "", &indicesData) // Etag: "" - we need to overwrite the file and create a new version
	if err != nil {
		return nil, err
	}

	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return nil, err
	}

	// //////////////////////////////////////////////////
	// execute the transaction steps
	// //////////////////////////////////////////////////
	var newEtag *string
	newEtag, err = r.executeTransactionSteps(ctx, transaction, id, 0)
	if err != nil {
		return nil, err
	}

	// update the transaction again, now that the ETags are known
	// if this step fails, we can still rollback, because we use the meta data in such cases to find the right version to remove
	err = r.updateTransaction(ctx, transaction)
	if err != nil {
		return nil, err
	}

	return newEtag, nil
}

// sql: delete from table_name where id = ?
// does an optimistically locked delete on an entity.
// If the ETag is wrong this method returns a StaleObjectError.
// If the ETag is '*', an error is returned.
// If the object doesn't exist this method does NOT return an error.
// Only the Id field of the entity is relevant.
func (r *MinioRepository) DeleteFromTable(ctx context.Context, transaction *schema.Transaction, table schema.Table, entity any, etag *string) error {

	if err := transaction.IsOk(); err != nil {
		return err
	}

	if *etag == "*" {
		return fmt.Errorf("ADB0032 ETag is '*', which is not allowed for delete.")
	}

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

	err = transaction.AddStep("delete-data", "application/json", table.Path(id), *etag, nil) // nil entity, so that we create a tombstone
	if err != nil {
		return err
	}

	// //////////////////////////////////////////////////
	// read existing indices to know what to delete
	// //////////////////////////////////////////////////
	object, err := r.Client.GetObject(ctx, r.BucketName, table.IndicesPath(id), minio.GetObjectOptions{})
	if err != nil {
		return err
	}
	defer object.Close()
	b, err := io.ReadAll(object)
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.StatusCode == http.StatusNotFound && *etag == "" {
			// OK - can happen if already deleted
			b = []byte("\"\"") // empty json string
		} else {
			return err
		}
	}
	// it is a json string => convert back to a normal one
	var existingIndicesAsString string
	if len(b) == 0 {
		existingIndicesAsString = ""
	} else {
		err = json.Unmarshal(b, &existingIndicesAsString)
		if err != nil {
			return err
		}
	}
	existingIndices := strings.Split(strings.TrimSpace(existingIndicesAsString), "\n")

	// //////////////////////////////////////////////////
	// calculate which ones to delete
	// //////////////////////////////////////////////////
	for _, existingIndex := range existingIndices {
		if existingIndex == "" { // happens when double deletion occurs and existingIndices is an empty string
			continue
		}

		// ETag: "" - not relevant for deletion
		err = transaction.AddStep("delete-remove-index", "text/plain", existingIndex, "", nil)
		if err != nil {
			return err
		}
	}

	// //////////////////////////////////////////////////
	// handle reverse indices
	// //////////////////////////////////////////////////
	indicesPath := table.IndicesPath(id)
	err = transaction.AddStep("delete-reverse-indices", "text/plain", indicesPath, "", nil) // Etag: "" - we need to overwrite the file and create a new version
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
	_, err = r.executeTransactionSteps(ctx, transaction, id, -1)
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

func (r *MinioRepository) executeTransactionSteps(ctx context.Context, transaction *schema.Transaction, id string, indexOfStepForWhichToReturnETag int) (*string, error) {
	var etag *string
	var executedStepCount = -1
	for _, step := range transaction.Steps {
		if step.Executed {
			continue
		} else {
			step.Executed = true
			executedStepCount++
		}

		opts := minio.PutObjectOptions{
			ContentType: step.ContentType,
			// TODO useful for auditing, when versioning is enabled
			// UserMetadata: map[string]string{
			// 	"who, what, when, for auditing purposes, same for updates": id,
			// },
			UserMetadata: step.UserMetadata,
		}
		if step.InitialETag == "" {
			// ignore, since the caller wants to overwrite in all cases
		} else if step.InitialETag == "*" {
			opts.SetMatchETagExcept(step.InitialETag) // match anything except "everything" => fail if exists
		} else {
			opts.SetMatchETag(step.InitialETag)
		}

		/* TODO unsure if we can set ETag requirements per key?
		fanOutReq := minio.PutObjectFanOutRequest{
			Entries: []minio.PutObjectFanOutEntry{

			},
		}
		r.Client.PutObjectFanOut(ctx, r.BucketName, a, fanOutReq)
		TODO unsure what "a" should be
		TODO cannot remove with this request, can only put
		*/
	
		if step.Type == "update-remove-index" ||
			step.Type == "delete-remove-index" {
			// left for the time being, removed on commit.
			// that way, other transactions can still find the object based on the old index entries.i.e. snapshot isolation.
		} else if step.Type == "insert-data" || // create new object
				  step.Type == "insert-add-index" || // create new object
				  step.Type == "insert-reverse-indices" || // create new object
				  step.Type == "update-data" || // create new version of object
				  step.Type == "update-add-index" || // create new object
				  step.Type == "update-reverse-indices" || // create new version of object
				  step.Type == "delete-data" || // create new version of object which is empty
				  step.Type == "delete-reverse-indices" { // create new version of object which is empty

			uploadInfo, err := r.Client.PutObject(ctx, r.BucketName, step.Path, bytes.NewReader(*step.Data), int64(len(*step.Data)), opts)
			if err != nil {
				respErr := minio.ToErrorResponse(err)
				if respErr.StatusCode == http.StatusPreconditionFailed {
					if step.InitialETag == "*" { // "must not exist", i.e. insert
						// if no transaction that is in progress added this object, it's a duplicate key.
						// if a different transaction that is also in progress added this object, it's a stale object (they got there first and we will fail assuming they commit).

						transactionsInProgress, err := r.getOtherTransactionsInProgress(ctx, transaction)
						if err != nil {
							return nil, err
						}
						for object := range r.Client.ListObjects(ctx, r.BucketName, minio.ListObjectsOptions{
							Prefix: step.Path,
							WithVersions: true,
							WithMetadata: true,
						}) {
							if object.Err != nil {
								return nil, object.Err
							}
							objectTxId := object.UserMetadata[MINIO_META_PREFIX+schema.TX_ID]
							for id, timeoutMicros := range transactionsInProgress {
								if id == objectTxId {
									return nil, &ObjectLockedErrorWithDetails[any]{Details: fmt.Sprintf("Object %s has already been written by transaction %s, which is set to expire by %d. Reload and try again.", step.Path, objectTxId, timeoutMicros), Object: step.Entity, DueByMsEpoch: timeoutMicros}
								}
							}
						}	
						return nil, &DuplicateKeyErrorWithDetails{Details: fmt.Sprintf("object %s already exists", step.Path)}
					} else if step.InitialETag != "" { // not "can be anything", i.e. must match, i.e. an update or delete
						return nil, &StaleObjectErrorWithDetails[any]{Details: fmt.Sprintf("object %s is stale. Reload and try again. Note, a different transaction that is also in progress may be writing to this object.", step.Path), Object: step.Entity}
					}
				}
				return nil, fmt.Errorf("ADB-0019 failed to put object with Id %s to path %s: %w", id, step.Path, err)
			}
	
			// update, so that commit/rollback can be more efficient
			step.FinalETag = &uploadInfo.ETag
			step.FinalVersionId = &uploadInfo.VersionID

			if executedStepCount == indexOfStepForWhichToReturnETag {
				etag = step.FinalETag
			}
		} else {
			return etag, fmt.Errorf("ADB-0001 Unexpected transaction step %s, please contact abstratium", step.Type)
		}

		// /////////////////////////////////////
		// update cache
		// /////////////////////////////////////
		// insert and update data are added
		if(step.Type == "insert-data") {
			transaction.Cache[step.Path] = &schema.ObjectAndETag{Object: step.Entity, ETag: step.FinalETag}
		} else if (step.Type == "update-data") {
			transaction.Cache[step.Path] = &schema.ObjectAndETag{Object: step.Entity, ETag: step.FinalETag}

		// deleted data are removed
		} else if (step.Type == "delete-data") {
			transaction.Cache[step.Path] = nil

		// insert and new update indices are added (delete never adds indices)
		// yes, indices are also cached, since we add from the cache when inspecting the index entries
		} else if (step.Type == "insert-add-index") {
			transaction.Cache[step.Path] = &schema.ObjectAndETag{Object: step.Entity, ETag: step.FinalETag}
		} else if (step.Type == "update-add-index") {
			transaction.Cache[step.Path] = &schema.ObjectAndETag{Object: step.Entity, ETag: step.FinalETag}

		// old update indices are removed, because we shouldn't find entries based on old indices, at least not within the current transaction that change the index
		} else if (step.Type == "update-remove-index") {
			transaction.Cache[step.Path] = nil
		} else if (step.Type == "delete-remove-index") {
			transaction.Cache[step.Path] = nil

		// reverse indices are not added
		} else if (step.Type == "insert-reverse-indices") {
		} else if (step.Type == "update-reverse-indices") {
		} else if (step.Type == "delete-reverse-indices") {

		} else {
			return nil, fmt.Errorf("ADB-0003 Unexpected transaction step type %s, please contact abstratrium", step.Type)
		}

	}

	return etag, nil
}

// sql: select * from table_name where column1 = value1 (column1 is in an index)
func (r *MinioRepository) selectPathsFromTableWhereIndexedFieldEquals(ctx context.Context, transaction *schema.Transaction, table schema.Table, fieldName string, value string) (*util.MutList[string], error) {
	matchingPaths := util.NewMutList[string]()
	// use the index definition to find the path of the actual record containing the data that the caller wants to read
	index, err := table.GetIndex(fieldName)
	if err != nil {
		return matchingPaths, err
	}
	indexPath := index.PathNoId(value) + "/" // add a slash since we don't do a recursive search, and without it, it just returns the folder, not the files in the folder

	// only need one per path. but there can be multiple version, so only select the one that is older than the transaction start time
	relevantPaths := make(map[string]bool) // effectively a set

	errors := util.NewMutList[error]()

	otherTransactionsInProgress, err := r.getOtherTransactionsInProgress(ctx, transaction)
	if err != nil {
		return nil, err
	}
	transactionIdsToIgnore := make([]string, 0, 10)
	for id := range otherTransactionsInProgress {
		transactionIdsToIgnore = append(transactionIdsToIgnore, id)
	}

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
			// last modified is used to ignore index entries that are created after the transaction started,
			// since snapshot isolation requires that we see the state of the database as it was at the start 
			// of the transaction, not afterwards.
			lastModifiedFromMetadata := object.UserMetadata[MINIO_META_PREFIX+schema.LAST_MODIFIED]
			lastModified, err := strconv.ParseInt(lastModifiedFromMetadata, 10, 64)
			if err != nil {
				errors.Add(err)
			}

			// tombstoned index entries are ones which no longer exist because of an update or delete, but
			// which are left in place until they expire, so that transactions that started before the update
			// can still use them.
			tombstoneFromMetadata := object.UserMetadata[MINIO_META_PREFIX+TOMBSTONE_AND_EXISTS_UNTIL]
			if tombstoneFromMetadata != "" {
				tombstone, err := strconv.ParseInt(tombstoneFromMetadata, 10, 64)
				if err != nil {
					errors.Add(err)
				}
				if tombstone < transaction.StartMicroseconds {
					// ignore tombstoned index entries - they are cleared away using the garbage collector
					continue
				}
			}

			if lastModified < transaction.StartMicroseconds {
				// ignore other transactions that are still in progress
				if !slices.Contains(transactionIdsToIgnore, object.UserMetadata[MINIO_META_PREFIX+schema.TX_ID]) {
					relevantPaths[object.Key] = true
				}
			}
		}
	}

	// add anything from the cache that matches the path, because those have a LastModified in Minio that
	// is newer than the tx start, but they are still relevant
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
		return "", fmt.Errorf("ADB-0020 expected a struct, got %s", v.Kind())
	}

	// Get the field by name
	field := v.FieldByName(fieldName)
	if !field.IsValid() {
		return "", fmt.Errorf("ADB-0021 no such field: %s", fieldName)
	}

	// check it is a string, otherwise create an error
	if field.Kind() != reflect.String {
		return "", fmt.Errorf("ADB-0022 field %s is not a string", fieldName)
	}

	return field.Interface().(string), nil
}

// returns a map of txId to timeoutMicros, excluding the given transaction
func (r *MinioRepository) getOtherTransactionsInProgress(ctx context.Context, tx *schema.Transaction) (map[string]uint64, error) {
	transactionsInProgress := make(map[string]uint64, 10)
	for object := range r.Client.ListObjects(ctx, r.BucketName, minio.ListObjectsOptions{
		Prefix:       tx.GetRootPath(),
		Recursive: false,
	}) {
		if object.Err != nil {
			return nil, object.Err
		}
		id, timeoutMicros := tx.GetIdAndTimeoutMicrosFromPath(object.Key)
		if id != tx.Id {
			transactionsInProgress[id] = timeoutMicros
		} // else exclude own, since we don't mind reading our own data
	}
	return transactionsInProgress, nil
}

// Returns the version of the object that was written before the transaction started, i.e. older than the transaction start timestamp.
// Ignores all versions from other transactions that are still in progress.
func (r *MinioRepository) readObjectVersionForTransaction(ctx context.Context, tx *schema.Transaction, path string) (*[]byte, *string, error) {
	// TODO move the following up a level and require that open transaction IDs are passed in, so that they aren't read multiple times?
	transactionIdsToIgnore := make([]string, 0, 10)
	var etag *string
	transactionsInProgress, err := r.getOtherTransactionsInProgress(ctx, tx)
	if err != nil {
		return nil, nil, err
	}

	// transactionsInProgress is a map of txId and timeoutMicros
	// ignore the timeout here and add all txIds that exist, since even though the tx has timed out, 
	// it hasn't yet rolled back, and such objects shouldn't be visible to other transactions.
	// we skip such IDs a little further down.
	for id := range transactionsInProgress {
		transactionIdsToIgnore = append(transactionIdsToIgnore, id)
	}

	versionToRead := ""
	for object := range r.Client.ListObjects(ctx, r.BucketName, minio.ListObjectsOptions{
		Prefix:       path, // full path of object we are reading
		WithVersions: true, // get version info so that we can find the object with a timestamp before the transaction started
		ReverseVersions: false, // latest first, iterate through the versions and break when we find one that is before the start of the tx and not part of a transaction that is still in progress
		WithMetadata: true,
	}) {
		if object.Err != nil {
			return nil, nil, object.Err
		}

		objectLastModifiedMicros := object.LastModified.UnixMicro()
		if objectLastModifiedMicros < tx.StartMicroseconds {
			// ignore other transactions that are still in progress
			if !slices.Contains(transactionIdsToIgnore, object.UserMetadata[MINIO_META_PREFIX+schema.TX_ID]) {
				versionToRead = object.VersionID
				etag = &object.ETag
				break
			}
		}
	}

	if versionToRead == "" {
		return nil, nil, &NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", path)}
	}

	// read the object using the exact version that we identified as being correct for this transaction
	objectData, err := r.Client.GetObject(ctx, r.BucketName, path, minio.GetObjectOptions{
		VersionID: versionToRead,
	})
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.StatusCode == http.StatusNotFound {
			return nil, nil, &NoSuchKeyErrorWithDetails{Details: fmt.Sprintf("object %s does not exist", path)}
		} else {
			return nil, nil, fmt.Errorf("ADB-0023 failed to get object with Path %s: %w", path, err)
		}
	}
	defer objectData.Close()

	b, err := io.ReadAll(objectData)
	if err != nil {
		return nil, nil, err
	}
	return &b, etag, nil
}

func (r *MinioRepository) BeginTransaction(ctx context.Context, timeout time.Duration) (schema.Transaction, error) {
	if timeout.Microseconds() > MAX_TX_TIMEOUT_MICROS {
		return schema.Transaction{}, fmt.Errorf("ADB-0024 timeout %d is too long, max is %d", timeout.Microseconds(), MAX_TX_TIMEOUT_MICROS)
	}
	tx := schema.NewTransaction(timeout)
	err := r.updateTransaction(ctx, &tx)
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.StatusCode == http.StatusPreconditionFailed {
			return tx, &DuplicateKeyErrorWithDetails{Details: fmt.Sprintf("transaction %s already exists", tx.Id)}
		} else {
			return tx, fmt.Errorf("ADB-0025 failed to put transaction %s: %w", tx.Id, err)
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
	uploadInfo, err := r.Client.PutObject(ctx, r.BucketName, transaction.GetPath()+"/"+TX_FILENAME, bytes.NewReader(json), int64(len(json)), opts)
	if err != nil {
		respErr := minio.ToErrorResponse(err)
		if respErr.StatusCode == http.StatusPreconditionFailed {
			return &DuplicateKeyErrorWithDetails{Details: fmt.Sprintf("transaction %s already exists", transaction.Id)}
		} else {
			return fmt.Errorf("ADB-0026 failed to put transaction %s: %w", transaction.Id, err)
		}
	}
	transaction.Etag = uploadInfo.ETag
	return nil
}

func (r *MinioRepository) GetTransactionsInProgress(ctx context.Context, transactions *[]schema.Transaction) error {
	// read all objects in the transactions folder
	tx := schema.NewTransaction(0)
	objectCh := r.Client.ListObjects(ctx, r.BucketName, minio.ListObjectsOptions{
		Prefix:    tx.GetRootPath(),
		Recursive: false,
	})
	for object := range objectCh {
		if object.Err != nil {
			return object.Err
		}

		// read the actual tx
		txData, err := r.Client.GetObject(ctx, r.BucketName, object.Key+TX_FILENAME, minio.GetObjectOptions{})
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

func (r *MinioRepository) Commit(ctx context.Context, tx *schema.Transaction) []error {
	if err := tx.IsOk(); err != nil {
		return []error{err} // do not wrap with fmt.Errorf...
	}
	errs := make([]error, 0, 10) // remove as much as possible
	tx.State = "Committing"
	err := r.updateTransaction(ctx, tx) // store in case this process fails and needs recovering
	if err != nil {
		errs = append(errs, fmt.Errorf("ADB-0004 Failed to update tx file %s during commit. %w", tx.GetPath(), err))
		return errs // fail fast
	}

	// go through each transaction step in reverse order and delete exactly that version
	for i := len(tx.Steps) - 1; i >= 0; i-- {
		step := tx.Steps[i]

		if step.Type == "update-remove-index" {
			// turn it into a "tombstone" and mark it to be cleared up at a later date.
			// it still needs to be around for any active transactions (potentially on different pods)
			// so that we fulfil snapshot isolation, and they can find records as they were at the start
			// of their transaction.

			until := fmt.Sprintf("%d", time.Now().Add(MAX_TX_TIMEOUT_MICROS * time.Microsecond).UnixMicro())

			// first create a garbage collection entry for the index
			contents := []byte(step.Path)
			gcPath := GC_ROOT + until
			_, err := r.Client.PutObject(ctx, r.BucketName, gcPath, bytes.NewReader(contents), int64(len(contents)), minio.PutObjectOptions{})
			if err != nil {
				errs = append(errs, fmt.Errorf("ADB-0016 Failed to put gc entry at path %s, %w", gcPath, err))
			}

			// then create the tombstone version of the index entry
			step.UserMetadata[TOMBSTONE_AND_EXISTS_UNTIL] = until
			opts := minio.PutObjectOptions{
				ContentType: step.ContentType,
				UserMetadata: step.UserMetadata,
			}
			_, err = r.Client.PutObject(ctx, r.BucketName, step.Path, bytes.NewReader([]byte("")), int64(0), opts)
			if err != nil {
				errs = append(errs, fmt.Errorf("ADB-0005 Failed to put tombstone object at path %s, %w", step.Path, err))
			}
		} // else no others are touched during commit
	}

	if len(errs) == 0 {
		// delete the transaction
		governanceBypass := true // transactions are not subject to governance
		if err := r.DeleteFolder(ctx, tx.GetPath(), governanceBypass, true); err != nil {
			errs = append(errs, fmt.Errorf("ADB-0006 Failed to remove tx during commit %s, %w", tx.GetPath(), err))
		}
	}
	return errs
}

func (r *MinioRepository) Rollback(ctx context.Context, tx *schema.Transaction) []error {
	if err := tx.IsOk(); err != nil {
		if errors.Is(err, schema.TransactionTimedOutError) {
			// ok, let the caller roll it back
		} else {
			return []error{err} // do not wrap with fmt.Errorf...
		}
	}

	tx.State = "RollingBack"
	err := r.updateTransaction(ctx, tx) // store in case this process fails and needs recovering
	if err != nil {
		return []error{err}
	}

	errs := make([]error, 0, 10) // remove as much as possible
	// go through each transaction step in reverse order and delete exactly that version
	for i := len(tx.Steps) - 1; i >= 0; i-- {
		step := tx.Steps[i]

		if step.Type == "insert-data" || // remove the newly inserted version of the object
		   step.Type == "insert-reverse-indices" || // exists for the object key, containing the current list of index files - remove version that was added
		   step.Type == "update-data" || // remove the version that was updated
		   step.Type == "update-reverse-indices" { // exists for the object key, containing the current list of index files - remove version that was added
			
			// ok, there really should only ever be one version, but let's be paranoid in the case that we were unable to
			// update the transaction after putting objects. or a better example: two updates in a transaction where the
			// second time we failed to update the tx file. then we could find multiple reverse indice file versions that
			// need cleaning up.
			versionIds := make([]string, 0, 10)
			if step.FinalVersionId != nil {
				versionIds = append(versionIds, *step.FinalVersionId)
			}
	
			if len(versionIds) == 0 {
				// we were unable to update the transaction file, but, we can search for anything with the transactionId in the metadata and delete that, because that metadata was added by before we knew the version number
				for object := range r.Client.ListObjects(ctx, r.BucketName, minio.ListObjectsOptions{
					Prefix: step.Path,
					WithVersions: true,
					WithMetadata: true,
				}) {
					if object.Err != nil {
						errs = append(errs, fmt.Errorf("ADB-0007 Error listing objects on path %s during rollback of tx %s, %w", step.Path, tx.GetPath(), object.Err))
					} else {
						// delete it, if the metadata matches
						// not working: var metaDataTxId string = object.UserMetadata[schema.TX_ID]
						var metaDataTxId string = object.UserMetadata[MINIO_META_PREFIX+schema.TX_ID]
						if metaDataTxId == step.UserMetadata[MINIO_META_PREFIX+schema.TX_ID] {
							versionIds = append(versionIds, object.VersionID)
						}
					}
				}
			}
	
			var wg sync.WaitGroup
			wg.Add(len(versionIds))
			var mu sync.Mutex
			for _, versionId := range versionIds { // yeah, normally there is one. but just in case we ever had more...
				go func(versionId string) {
					defer wg.Done()
					err := r.Client.RemoveObject(ctx, r.BucketName, step.Path, minio.RemoveObjectOptions{
						VersionID: versionId,
					})
					if err != nil {
						mu.Lock()
						defer mu.Unlock()
						errs = append(errs, fmt.Errorf("ADB-0008 Failed to remove object at path %s during rollback of tx %s, %w", step.Path, tx.GetPath(), err))
					}
				}(versionId)
			}
			wg.Wait()
		} else if step.Type == "insert-add-index" || // index files either exist, or they don't. they have no versioned content.
		          step.Type == "update-add-index" { // index files either exist, or they don't. they have no versioned content.

			err := r.Client.RemoveObject(ctx, r.BucketName, step.Path, minio.RemoveObjectOptions{
				ForceDelete: true,
				GovernanceBypass: true,
			})
			if err != nil {
				errs = append(errs, fmt.Errorf("ADB-0009 Failed to remove object at path %s during rollback of tx %s, %w", step.Path, tx.GetPath(), err))
			}
		} else if step.Type == "update-remove-index" {
			// not used during rollback
		} else {
			errs = append(errs, fmt.Errorf("ADB-0002 Unexpected transaction step type %s, please contact abstratium", step.Type))
		}
	}

	if len(errs) == 0 {
		governanceBypass := true // transactions are not subject to governance
		err := r.DeleteFolder(ctx, tx.GetPath(), governanceBypass, true)
		if err != nil {
			errs = append(errs, fmt.Errorf("ADB-0010 Failed to remove tx during rollback %s, %w", tx.GetPath(), err))
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
		return fmt.Errorf("ADB-0028 Finished deletion process with %d errors. All errors were:\n%s", len(errors), errorString)
	}
	return nil
}
