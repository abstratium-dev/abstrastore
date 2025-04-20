# Abstrastore

A very simple library that encapsulates a database using Minio as a file store. Designed to be embedded in multiple instances
of a microservice which read and write concurrently. The design is based on an append log.
It does not support ACID. The store is periodically compacted to improve read rate and cost.


## Roadmap

- caching and cache eviction
- observability
- metrics

## Design

### E-Tags

E-Tags are used to ensure that data is not overwritten by another instance.
If a write fails, then a retry is attempted.

### WAL

A write ahead log (WAL) is used to ensure that data is not overwritten by another instance.
Each instance of a microservice knows how much of the WAL it has read and anytime a client
reads or writes, it re-reads the rest of the WAL to make sure that it is up to date and can apply
those changes in memory.

### Buckets

One bucket per microservice. Multiple microservice instances (pods; docker containers of the same image) use the same bucket.

### Schemas

Within the bucket, there are "folders" for each schemas.

### Tables

Within the schema folder, there is one "folder" for each table.
That folder contains two sub-folders: one for indices and one for the actual table.

Tables are stored under `<schema>/<table>/_data`.

Tables are stored in page files, which contain up to the configurable number of entries.

Indexes reference those page files.

Page files are stored under `<schema>/<table>/_data/page-<prefix>.json`.

TODO how do we update an index and a page file *atomically*?
Use a write ahead log (WAL).

#### Writes

- fail if the file already exists

#### Updates

- fail if the etag has changed and then reload, remerge and retry
  - remerge means update the fields that the caller is trying to update
  - we can merge if the fields being updated are not the ones being updated by the caller
  - if they are, throw an optimistic lock exception so that the user can reload and retry
  - hmm jpa has optimistic locking per row, not per field

### Indices

A fundamental design decision is that the keys that are indexed are randomly distributed.
If they are not, then shard files will be unevenly sized.

Indices are stored under `<schema>/<table>/_indexes/<index-name>`.
There is a manifest file for each index, stored under `<schema>/<table>/_indexes/<index-name>/manifest.json`.
It contains metadata like the depth of the index shards. 

Indexes are split into shards, stored under `<schema>/<table>/_indexes/<index-name>/shard-<prefix>.json`.
The prefix describes the keys that can be found in that shard.

Shards are not supposed to have more than the configured number of entries. If they do have more, then the
entire index needs to be rebuilt by splitting it into an extra layer.
This could be designed so that a single microservice instance decides that it will rebuild the index. It would reserve the right
to rebuild by creating a file named `<schema>/<table>/_indexes/<index-name>/rebuild`, which would contain its hostname.
If that file were to exist, others could not write it. The microservice in question would then rebuild the index by creating all the
new shards. The problem is however, how do we then cope with other service instances writing to the old index at the same time? 
That would have to cause the new index files related to that old shard to become invalidated.

*So for the moment, let's leave it so that re-indexing is something that has to happen in a maintenance window when the service is offline.*

## License

Apache 2.0 => see [LICENSE](LICENSE)

## Authors

Ant Kutschera

## Infrastructure

### Minio

```sh
mkdir -p /temp/minio-data

docker run \
    --rm -it \
    --name minio \
    -p 127.0.0.1:9000:9000 \
    -p 127.0.0.1:9090:9090 \
    -v "/temp/minio-data:/data:rw" \
    -e "MINIO_ROOT_USER=rootuser" \
    -e "MINIO_ROOT_PASSWORD=rootpass" \
    quay.io/minio/minio:RELEASE.2024-09-13T20-26-02Z server /data --console-address ":9090" --anonymous
```

web-ui: http://127.0.0.1:9090

```sh
rm -f /tmp/cookies.txt
curl -vvv --location 'http://127.0.0.1:9090/api/v1/login' \
        --silent \
        --header 'Content-Type: application/json' \
        --cookie-jar /tmp/cookies.txt \
        --data '{"accessKey":"rootuser","secretKey":"rootpass"}'

curl 'http://127.0.0.1:9090/api/v1/buckets' \
    -vvv \
    -X 'POST' \
    --header 'Content-Type: application/json' \
    --cookie /tmp/cookies.txt \
    --data-raw '{"name":"abstrastore-tests","versioning":{"enabled":false,"excludePrefixes":[],"excludeFolders":false},"locking":false}' \
    | json_pp
```

## Building / Releasing

```sh
eval "$(ssh-agent -s)"
ssh-add /.../abs.key
export VERS=0.0.x
git add --all && git commit -a -m'<comment>' && git tag v${VERS} && git push origin main v${VERS}
```

## TODO

- use local minio, not remote => to test latency
- add using https://pkg.go.dev/about#adding-a-package
- sql parsing - https://github.com/xwb1989/sqlparser
