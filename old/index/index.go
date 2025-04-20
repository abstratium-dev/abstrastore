package index

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"time"

	"github.com/minio/minio-go/v7"
)

// IndexEntry represents the index structure you want to store
type IndexEntry map[string]string // or use a struct if needed

// MaxRetries defines how many times to retry in case of ETag mismatch
const MaxRetries = 3

// ReadIndex reads the index file and returns the content and ETag
func ReadIndex(ctx context.Context, client *minio.Client, bucket, objectKey string) (IndexEntry, string, error) {
	obj, err := client.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", err
	}
	defer obj.Close()

	stat, err := obj.Stat()
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return make(IndexEntry), "", nil // treat missing as empty index
		}
		return nil, "", err
	}

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", err
	}

	var index IndexEntry
	if len(data) > 0 {
		if err := json.Unmarshal(data, &index); err != nil {
			return nil, "", err
		}
	} else {
		index = make(IndexEntry)
	}

	return index, stat.ETag, nil
}

// WriteIndex attempts to write the index using an ETag for optimistic locking
func WriteIndex(ctx context.Context, client *minio.Client, bucket, objectKey string, newIndex IndexEntry, oldETag string) error {
	var lastErr error

	for i := 0; i < MaxRetries; i++ {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(newIndex); err != nil {
			return err
		}

		opts := minio.PutObjectOptions{
			ContentType: "application/json",
		}
		opts.SetMatchETag(oldETag)

		_, err := client.PutObject(ctx, bucket, objectKey, &buf, int64(buf.Len()), opts)
		if err == nil {
			return nil // success
		}

		// Handle precondition failure (ETag mismatch)
		if minio.ToErrorResponse(err).StatusCode == http.StatusPreconditionFailed {
			// Refresh index and retry
			index, etag, err := ReadIndex(ctx, client, bucket, objectKey)
			if err != nil {
				return fmt.Errorf("failed to re-read index: %w", err)
			}

			// Here, merge logic would be applied depending on use case
			// For simplicity, we override it — you could customize this
			for k, v := range newIndex {
				index[k] = v
			}

			newIndex = index
			oldETag = etag
			lastErr = err
			time.Sleep(100 * time.Millisecond) // backoff before retry
			continue
		}

		return err
	}

	return fmt.Errorf("failed to update index after %d retries: %w", MaxRetries, lastErr)
}

type MergeFunc func(current, incoming IndexEntry) IndexEntry

func WriteIndexWithMerge(
	ctx context.Context,
	client *minio.Client,
	bucket, objectKey string,
	newIndex IndexEntry,
	oldETag string,
	mergeFn MergeFunc,
) error {
	var lastErr error

	for i := 0; i < MaxRetries; i++ {
		// Encode to JSON
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(newIndex); err != nil {
			return err
		}

		// Attempt to write with ETag
		opts := minio.PutObjectOptions{
			ContentType: "application/json",
		}
		opts.SetMatchETag(oldETag)

		_, err := client.PutObject(ctx, bucket, objectKey, &buf, int64(buf.Len()), opts)
		if err == nil {
			return nil // success
		}

		// Retry if ETag no longer matches
		if minio.ToErrorResponse(err).StatusCode == http.StatusPreconditionFailed {
			// Re-read the latest version
			currentIndex, newETag, err := ReadIndex(ctx, client, bucket, objectKey)
			if err != nil {
				return fmt.Errorf("failed to re-read index: %w", err)
			}

			// Merge using custom strategy
			newIndex = mergeFn(currentIndex, newIndex)
			oldETag = newETag

			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}

		return err
	}

	return fmt.Errorf("failed to update index after %d retries: %w", MaxRetries, lastErr)
}

// Default override (replace on conflict)
func OverrideMerge(current, incoming IndexEntry) IndexEntry {
	for k, v := range incoming {
		current[k] = v
	}
	return current
}

// Union merge (preserve all values)
func UnionMerge(current, incoming IndexEntry) IndexEntry {
	merged := make(IndexEntry)
	for k, v := range current {
		merged[k] = v
	}
	for k, v := range incoming {
		if _, exists := merged[k]; !exists {
			merged[k] = v
		}
	}
	return merged
}

type IndexShard struct {
	Entries IndexEntry
	ETag    string
	Name    string
}

const MaxEntriesPerShard = 5 // TODO set to 1_000 after testing

func ReadIndexShards(ctx context.Context, client *minio.Client, bucket, prefix string) ([]IndexShard, error) {
	var shards []IndexShard

	// List all objects under prefix
	for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if object.Err != nil {
			return nil, object.Err
		}

		// Get ETag + Content
		obj, err := client.GetObject(ctx, bucket, object.Key, minio.GetObjectOptions{})
		if err != nil {
			return nil, err
		}
		defer obj.Close()

		stat, err := obj.Stat()
		if err != nil {
			return nil, err
		}

		data, err := io.ReadAll(obj)
		if err != nil {
			return nil, err
		}

		var entries IndexEntry
		if err := json.Unmarshal(data, &entries); err != nil {
			return nil, err
		}

		shards = append(shards, IndexShard{
			Entries: entries,
			ETag:    stat.ETag,
			Name:    object.Key,
		})
	}

	return shards, nil
}

func WriteIndexShardsWithMerge(
	ctx context.Context,
	client *minio.Client,
	bucket string,
	prefix string,
	incoming IndexEntry,
	mergeFn MergeFunc,
) error {
	shards, err := ReadIndexShards(ctx, client, bucket, prefix)
	if err != nil {
		return err
	}

	// Merge all entries into one for now
	all := make(IndexEntry)
	for _, shard := range shards {
		all = mergeFn(all, shard.Entries)
	}
	all = mergeFn(all, incoming)

	// Split into shards
	sharded := splitIntoShards(all, MaxEntriesPerShard)

	// Write back with ETag match
	for _, shard := range sharded {
		var buf bytes.Buffer
		json.NewEncoder(&buf).Encode(shard.Entries)

		opts := minio.PutObjectOptions{
			ContentType: "application/json",
		}
		if shard.ETag != "" {
			opts.SetMatchETag(shard.ETag)
		}

		_, err := client.PutObject(ctx, bucket, shard.Name, &buf, int64(buf.Len()), opts)
		if err != nil {
			return fmt.Errorf("failed to write shard %s: %w", shard.Name, err)
		}
	}

	return nil
}

func splitIntoShards(index IndexEntry, maxSize int) []IndexShard {
	var shards []IndexShard
	shard := IndexEntry{}
	count := 0
	shardNum := 1

	for k, v := range index {
		shard[k] = v
		count++

		if count >= maxSize {
			shards = append(shards, IndexShard{
				Entries: shard,
				Name:    fmt.Sprintf("indexes/issues-by-user-%04d.json", shardNum),
			})
			shard = IndexEntry{}
			count = 0
			shardNum++
		}
	}

	if count > 0 {
		shards = append(shards, IndexShard{
			Entries: shard,
			Name:    fmt.Sprintf("indexes/issues-by-user-%04d.json", shardNum),
		})
	}

	return shards
}


type ShardInfo struct {
	Name     string   `json:"name"`
	KeyRange [2]string `json:"key_range"` // min, max key
	ETag     string   `json:"etag"`
	Count    int      `json:"count"`
}

type ShardManifest struct {
	Shards    []ShardInfo `json:"shards"`
	UpdatedAt time.Time   `json:"updated_at"`
}

func SaveShardManifest(ctx context.Context, client *minio.Client, bucket, manifestKey string, shards []IndexShard) error {
	var infos []ShardInfo

	for _, shard := range shards {
		keys := make([]string, 0, len(shard.Entries))
		for k := range shard.Entries {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		infos = append(infos, ShardInfo{
			Name:     shard.Name,
			KeyRange: [2]string{keys[0], keys[len(keys)-1]},
			ETag:     shard.ETag,
			Count:    len(keys),
		})
	}

	manifest := ShardManifest{
		Shards:    infos,
		UpdatedAt: time.Now().UTC(),
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(manifest); err != nil {
		return err
	}

	_, err := client.PutObject(ctx, bucket, manifestKey, &buf, int64(buf.Len()), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	return err
}
