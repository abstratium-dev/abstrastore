# Abstrastore

A library that encapsulates an MVCC based object database using Minio / S3 as a file store.

Designed to be embedded in multiple instances of a microservice (containers/pods) which read and write concurrently.

Reduces the costs of running databases in the cloud by using only blob storage.

Supports "repeatable read" with snapshot isolation and optimistic locking.

Supports reading by primary key and configurable indexing on all fields.

Supports atomic operations, e.g. multiple write operations in a transaction (Create, Update, Delete; CUD).

Acts as an object store rather than a document store, meaning that it doesn't support schemas or schema validation (although that could be implemented by the application)

Supports database migrations which are implemented as versioned algorithms in your application.

Primary keys must always be unique.

## Motivation

- Blob storage is much cheaper than an SQL database in the cloud.
- We don't want to install any extra infrastructure, just our microservices and either Minio or subscribe to a Minio compatible service like S3.
- The solution should be cheap to run, e.g. with Google Cloud Run and Minio.


## Non-Functional Requirements / Quality Attributes

- The solution must provide ACID properties.
- It must support multiple instances of a service (container/pod) reading and writing concurrently.
- It must support recovery in the case of a crash.
- It must support transactions.
- It must support database migrations.
- It must support REPEATABLE READ isolation level, or better, e.g. snapshot isolation.
- It must support optimistic locking out of the box. Any CUD operation will fail if a different transaction has already modified the object in question within it's own transaction.

## Roadmap

- caching and cache eviction
- conflict resolution? it would be easy to have a callback from the library to client code which deals with reconcilliation, and the lib could also offer "last writer wins", by default. See https://en.wikipedia.org/wiki/Eventual_consistency#Conflict_resolution
  - but is it necessary, with our strategy to fail fast with optimistic locking?
- observability
- metrics


## License

Apache 2.0 => see [LICENSE](LICENSE)

## Copyright

Copyright (c) 2025 abstratium informatique sàrl

## Authors / Contributors

Ant Kutschera, abstratium informatique sàrl

## Infrastructure for Testing

### Minio

```sh
mkdir -p /temp/minio-data

docker run \
    --rm -it \
    --name abstratium-minio \
    --network abstratium \
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

## Links

- ACID Wikipedia: https://en.wikipedia.org/wiki/ACID
- Eventual consistency Wikipedia: https://en.wikipedia.org/wiki/Eventual_consistency
- Eventually Consistent by Werner Vogels (2009): https://dl.acm.org/doi/pdf/10.1145/1435417.1435432
- Notes on distributed databases (1979): https://dominoweb.draco.res.ibm.com/reports/RJ2571.pdf
- Nice article on MVCC in MySql: https://www.red-gate.com/simple-talk/databases/mysql/exploring-mvcc-and-innodbs-multi-versioning-technique/
- Article on MVCC in Postgres: https://www.postgresql.org/docs/current/mvcc-intro.html and https://www.postgresql.org/docs/current/transaction-iso.html
- Article about how MVCC works: https://nagvekar.medium.com/understanding-multi-version-concurrency-control-mvcc-in-postgresql-a-comprehensive-guide-9b4f82153860

## TODO

- PutObjectFanOut for writing multiple files at the same time? more efficient?
-	how do deleted objects work in minio? when there is no data??
- when committing/rolling back, we could update the file so that the exipiry time is set to in 100ms that way if the process were to die, another instance would very quickly complete the transaction
- fix docs/design.drawio (keep the old on in an archive folder as an alternative design)
- document limitations from https://min.io/docs/minio/linux/operations/concepts/thresholds.html, e.g. field value in index may not be too long
- improve: min.NewTypedQuery(repo, context.Background(), &tx2, &Account{}).SelectFromTable(T_ACCOUNT) => if T_ACCOUNT had a reference to the template, we wouldn't need to provide it in the typed query?
- rename to abstraDB?
- test what happens if you update an object that has in the mean time been deleted. should get a StaleObjectError.
- add delete
- add upsert
- range index search, e.g. more than or between, rather than exact matches
- index search with regex
- add all CRUD stuff to transaction cache, so that its repeatable read
- use UserMetadata more, since we get it when listing objects, not just reading objects
- explain that we use the index prefix of two letters so that you can do autocomplete searches
- make interface have insert, upsert, update. insert and update fail if the object exists, or doesn't exist respectively.
- document using etags when updating
- think of scenarios which are not covered by the current implementation and document them
  - how can we test stuff like that? by forcing the lib to wait for a certain time during testing, part way thru its process
- so how is BASE eventually consistent, if it has no transactions?
  - base will keep trying to write to other nodes and so eventually they will be consistent. that isn't the problem we are trying to solve. rather, we don't support transactions and so if a system failure occurs during a set of writes (a process) then the system will not know about the other things that the user wanted to do.  it can however try and ensure that the indexes are up to date with the data, using the mechanism described above which writes a single file containing all intentions, before then executing them.
- do load and performance tests
  - check how many concurrent transactions can be handled because we always need to check index and file versions against them in order to ignore non-committed data
- add using https://pkg.go.dev/about#adding-a-package
- sql parsing - https://github.com/xwb1989/sqlparser
- put internal structs into internal package
  - create an api, everything else should be hidden in internal packages
- make cleaning up old versions configurable in order to support auditing requirements

