# Design

## E-Tags

E-Tags are used to ensure that data is not overwritten by another instance.

## Write Ahead Log (WAL)

None currently used.

## Buckets

One bucket per microservice image. Multiple microservice instances (pods; docker containers of the same image) 
use the same bucket.

## Databases

Within the bucket, there are "folders" for each database.

## Tables

Within the database folder, there is one "folder" for each table.
That folder contains two sub-folders: one for `indices` and one for the actual rows (named `data`).

Tables are stored under `<database>/<tableName>/data`.

Each "row" (object) is stored as a "file" with its ID being the key of the object.

## Indices

Indices reference the table files.

Indexes are stored under `<database>/<tableName>/indices/<fieldName>/<firstTwoCharsOfValue>/<value>/<database>___<tableName>___<id>`.
That way you can search for the index entry by the value and get all database/table/id combinations that this index entry refers to.
The file itself is empty, because the filename contains all the information needed.

## Writes

- fail if the etag is wrong.
- fail if the etag has changed. simply informs the user of an optimistic locking exception. they should reload and retry.
- the application should be designed to be robust and idempotent.

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

From worst to best:

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

But wait! What about snapshot isolation, https://en.wikipedia.org/wiki/Snapshot_isolation which comes from https://en.wikipedia.org/wiki/Multiversion_concurrency_control? Yes, snapshot isolation should be our goal.
