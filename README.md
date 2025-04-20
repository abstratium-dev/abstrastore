# Abstrastore

A very simple library that encapsulates a database using Minio as a file store. Designed to be embedded in multiple instances
of a microservice (containers/pods) which read and write concurrently. 

Does not support transactions. Or ACID.

Supports reading by primary and foreign keys.
Supports indexes on all fields.

No current support for schema migrations.

## Roadmap

- caching and cache eviction
- observability
- metrics

## Design

### E-Tags

E-Tags are used to ensure that data is not overwritten by another instance.
If a write fails, then a retry is attempted. Used for updates.

### WAL

None currently used.

### Buckets

One bucket per microservice image. Multiple microservice instances (pods; docker containers of the same image) 
use the same bucket.

### Schemas

Use the "DSL" to describe the database schema.

### Databases

Within the bucket, there are "folders" for each database.

### Tables

Within the database folder, there is one "folder" for each table.
That folder contains two sub-folders: one for `indices` and one for the actual rows (named `data`).

Tables are stored under `<database>/<tableName>/data`.

Each "row" (object) is stored as a "file" with its ID being the key of the object.

### Indices

Indices reference the table files.

Indexes are stored under `<database>/<tableName>/indices/<fieldName>/<firstTwoCharsOfValue>/<value>/<database>___<tableName>___<id>`.
That way you can search for the index entry by the value and get all database/table/id combinations that this index entry refers to.
The file itself is empty, because the filename contains all the information needed.

#### Writes

- fail if the etag is wrong.

#### Updates

- fail if the etag has changed. simply informs the user of an optimistic locking exception. they should reload and retry.
- the application should be designed to be robust and idempotent.

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
