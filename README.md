# Abstrastore

A very simple library that encapsulates an eventually consistent database using Minio as a file store. Designed to be embedded in multiple instances of a microservice (containers/pods) which read and write concurrently. 

Does not support transactions or full ACID properties. It is durable and tries to be consistent.

Supports reading by primary and foreign keys.
Supports indexes on all fields.

No current support for schema migrations. You describe the schema with a DSL which is used at runtime to determine
what indexes to write. If you need to migrate data, you would add that to your program and temporarily have multiple
versions of your DSL.

## Motivation

- Blob storage is cheaper than an SQL database in the cloud.
- We don't want to install any extra infrastructure, just Minio and our microservices.
- The solution should be cheap to run, e.g. with Google Cloud Run and Minio.


## Non-Functional Requirements / Quality Attributes

- The solution should be eventually consistent.
- It must not fulfil ACID properties.
- It must support multiple instances of a service (container/pod) reading and writing concurrently.
- It does not need to support recovery in the case of a crash.
- It does not need to support transactions.
- It does not need to support schema migrations.
- Supports REPEATABLE READ isolation level.
- Supports atomic operations, e.g. multiple CUD operations in a transaction.
- Supports optimistic locking out of the box. Any CUD operation will fail if a different transaction has already modified the object in question within it's own transaction. While that transaction might still roll back, the library assumes it won't, and doesn't block the current transaction and instead fails fast, rolling back any changes made in that current transaction. See https://en.wikipedia.org/wiki/Multiversion_concurrency_control: "A Write cannot complete if there are other outstanding transactions with an earlier Read Timestamp (RTS) to the same object".
- TODO no wait, we support "snapshot isolation" => https://en.wikipedia.org/wiki/Snapshot_isolation which comes from https://en.wikipedia.org/wiki/Multiversion_concurrency_control

## Use Cases With A Good Fit

- Small applications where data is not read or written very frequently
  - Blob Stores / S3 are optimized for large-scale data storage and typically charge based on the amount of data stored, with additional costs for data retrieval and requests. It is often more cost-effective for storing large volumes of data that don't require frequent access or complex querying.
  - Databases like MongoDB and SQL databases usually charge based on the compute resources (CPU, memory) and storage used. They are designed for data that requires complex querying, transactions, and real-time access, which can make them more expensive for large-scale storage.
  - Use Case: S3 is ideal for static file storage, backups, and data lakes, where data retrieval is less frequent. Databases are better for applications requiring real-time data processing and complex queries.
  - Pricing Models: The exact cost comparison can vary based on the specific use case, data access patterns, and the cloud provider's pricing model. It's important to analyze the pricing details of each service to determine the cost-effectiveness for your specific needs.

## Further Thoughts

- Isolation is less important in systems where there are not many writes, because the chances of reading stale data are low.
- Atomicity is replaced by idempotency. If an operation fails, the client should retry the operation. Agreed, if a client doesn't retry, data could be inconsistent. But, we use a kind of WAL (write ahead logging) to ensure that data is eventually consistent, by initially writing a single file containing all changes that are to be made, and if that file exists longer than the timeout, then the operation is retried automatically by the library. Failures should be logged for an operator to take action.

## Isolation

### Dirty Reads

- https://en.wikipedia.org/wiki/Isolation_(database_systems)#Dirty_reads
- We aim to avoid dirty reads.

So we can achieve these isolation levels: READ COMMITTED, REPEATABLE READ, SERIALIZABLE

### Non-repeatable reads

- https://en.wikipedia.org/wiki/Isolation_(database_systems)#Non-repeatable_reads
- We cannot support this, because of the inconsistency window when we transfer the objects from the transaction path to their final path.

So we can achieve  READ COMMITTED or READ UNCOMMITTED. The latter is bad, because it can lead to dirty reads.

Or, if we used a local key based cache to see what has already been written / read, we can achieve REPEATABLE READ or SERIALIZABLE.

### Phantom reads

- https://en.wikipedia.org/wiki/Isolation_(database_systems)#Phantom_reads
- We cannot avoid this without either locking everything (serial, undesirable in terms of scalability) or copying all data. So at best we can achieve READ UNCOMMITTED, READ COMMITTED or REPEATABLE READ.

### Result

- READ UNCOMMITTED - undesired
- READ COMMITTED - possible
- REPEATABLE READ - possible if every read-by-key is checked against the transaction folder to see what has already been read
- SERIALIZABLE - unachievable and undesired

REPEATABLE READ is better than READ COMMITTED, since the former avoids dirty read and non-repeatable read, whereas the latter only avoids dirty read.

### Comparison to other DBs

https://www.linkedin.com/pulse/default-isolation-level-databases-sql-server-oracle-mysql-servino-b7lqf/

|DB|Default|Notes|
|---|---|---|
|PostgreSQL|READ COMMITTED|-
|MySQL|REPEATABLE READ|-
|Oracle|READ COMMITTED|-
|SQL Server|READ COMMITTED|-
|Azure SQL|READ COMMITTED|-

### Conclusion

We should aim for REPEATABLE READ.

But wait! What about snapshot isolation, https://en.wikipedia.org/wiki/Snapshot_isolation which comes from https://en.wikipedia.org/wiki/Multiversion_concurrency_control? 

## Roadmap

- caching and cache eviction
- conflict resolution? it would be easy to have a callback from the library to client code which deals with reconcilliation, and the lib could also offer "last writer wins", by default. See https://en.wikipedia.org/wiki/Eventual_consistency#Conflict_resolution
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


## TODO

- make interface have insert, upsert, update. insert and update fail if the object exists, or doesn't exist respectively.
- document using etags when updating
- think of scenarios which are not covered by the current implementation and document them
  - how can we test stuff like that? by forcing the lib to wait for a certain time during testing, part way thru its process
- atomicity as implemented here only refers to updating records and indexes, and does NOT refer to multiple inserts/updates/deletes at the same time -> since we have no transactions
- so how is BASE eventually consistent, if it has no transactions?
  - base will keep trying to write to other nodes and so eventually they will be consistent. that isn't the problem we are trying to solve. rather, we don't support transactions and so if a system failure occurs during a set of writes (a process) then the system will not know about the other things that the user wanted to do.  it can however try and ensure that the indexes are up to date with the data, using the mechanism described above which writes a single file containing all intentions, before then executing them.
- write an atomic file when inserting/updating data. this is used if the system crashes, to recover. that way we at least have consistency and durability. => this is a WAL, but deleted once the individual files are created
- make tradeoff between idempotence and recognising that the data is already there, configurable?
- describe how indexes are written idempotently and if two writes occur concurrently, then the last one will win, but only in the situation where the tx1 inserts data, tx2 inserts data, tx2 writes index files, tx1 writes index files.
- use local minio, not remote => to test latency
- add using https://pkg.go.dev/about#adding-a-package
- sql parsing - https://github.com/xwb1989/sqlparser
- which consistency do we want to support?
- versioning to support auditing requirements