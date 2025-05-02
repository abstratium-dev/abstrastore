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

Rollback can delete a previously inserted index by RemoveObject with the VersionID in the options.
If an update or delete of an object causes the index to change, we cannot just delete the old index because
the transaction isn't yet committed, and once removed, the object is really missing from Minio. Not only that, but
other transactions still need to be able to find objects using those old index entries.

TODO A better solution is to add indices, but only remove old once at the end of the transaction?
This should work, because any time that an object is read, we use the version that existed at the start of the transaction or null
if the object was only created after the transaction started.
Wait though... if I find an object using the new index, but that isn't committed, I would need to read the version of the object matching my transaction start time, and then determine that that version didn't actually match because its field was different.
That too forces us to always load the full object, not just its ID or header (e.g. from ListObject).
Ahh, we could put the target of the index in the file and if it's deleted, clear the contents like a tombstone. that way, when reading the index, you need to also look at the size of the file to determine if it's still valid.  alternatively we could write new version of the old index file to indicate that it is deleted, e.g. with metadata, e.g. DeletedWithTxId.  if a transaction determines that that tx is still active, it knows to ignore that index entry. commit then needs to actually remove that file.

```
INSERT
/db/t/data/id.json                        <<< write file, step=insert-data, cached
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< write file, step=insert-index, cached
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< write file, step=insert-index, cached
/db/t/data/id.indices                     <<< write file, used to delete old index entries, step=insert-reverse-indices

describe what gets cached.

ROLLBACK INSERT
/db/t/data/id.json                        <<< delete the version of this file
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< delete this file
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< delete this file
/db/t/data/id.indices                     <<< delete this file

COMMIT INSERT
/db/t/data/id.json                        <<< nothing to do
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< nothing to do
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< nothing to do
/db/t/data/id.indices                     <<< nothing to do

UPDATE (field value of field name 2 changes)
/db/t/data/id.json                        <<< write new version of file, step=update-data, cached
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< leave this file alone, the field value didn't change
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< leave this for the moment, since other txs must still find using old value. step=update-remove-index, de-cached
/db/t/indices/fn2/fv'[:2]/fv'/db___t___id <<< write file (the field value changed and this is the new index entry)**, step=update-add-index, cached
/db/t/data/id.indices                     <<< write new version of file containing new indices that would need modifying on UD*

* should it contain just fn1/fv and fn2/fv', or all three and be updated on commit? the nice thing is that no one else can see the latest version of the actual object and so they cannot get its etag and so they cannot update it. the only one able to update it again is the current transaction. the version of the actual object with the updated field cannot be selected because it's not committed yet and that version belongs to an open transaction and so will be ignored by all other transactions.

** set txid into metadata. anytime that index files are read, we always need to disregard entries related to transactions that are in progress, otherwise other tx will use them to read objects that do not match their predicate. it'll be interesting to see how many concurrent transactions still perform well.

ROLLBACK UPDATE
/db/t/data/id.json                        <<< delete the newly created version of this file
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< no need to do anything
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< no need to do anything
/db/t/indices/fn2/fv'[:2]/fv'/db___t___id <<< remove this, step=update-add-index
/db/t/data/id.indices                     <<< remove updated version of file

COMMIT UPDATE
/db/t/data/id.json                        <<< no need to do anything
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< no need to do anything
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< remove this now, step=update-remove-index, see above
/db/t/indices/fn2/fv'[:2]/fv'/db___t___id <<< no need to do anything
/db/t/data/id.indices                     <<< no need to do anything


DELETE
/db/t/data/id.json                        <<< create empty version or stick a tombstone header on it*
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< leave this for the moment, since other txs must still find using old value
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< leave this for the moment, since other txs must still find using old value
/db/t/data/id.indices                     <<< write new version with no content?

* other tx can then still read this, by reading an old version and ignoring this non committed version

ROLLBACK DELETE
/db/t/data/id.json                        <<< remove empty version created above
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< nothing to do
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< nothing to do
/db/t/data/id.indices                     <<< remove newly created version

COMMIT DELETE - has an inconsistency window
/db/t/data/id.json                        <<< remove all versions
/db/t/indices/fn1/fv[:2]/fv/db___t___id   <<< remove all versions
/db/t/indices/fn2/fv[:2]/fv/db___t___id   <<< remove all versions
/db/t/data/id.indices                     <<< remove all versions

```

What happens if a second transaction reads in-progress-transaction-ids and ignores data that belongs to them, which is then actually committed in the mean time? That is correct! MVCC and snapshot isolation ensures that a transaction is based on data that existed at the time that the transaction started. If it were to try and write to objects that have in the mean time been written by other transactions, it will fail with a StaleObjectError or NoSuchKeyError. Except fo the special case where one transaction deleted and the current transaction inserts after the commit of the delete. I would say that based on the snapshot, it shouldn't be able to insert, because at the time, a version of the object already existed! Is that bad? Hmmm... In tests, MySQL locks the deleted row in T1 which blocks the subsequent insert in T2. When T1 commits, T2 successfully inserts. Optimistic Locking applies to updates, not inserts. So the fact that abstraDB doesn't throw an error in that special case is correct. Phew! But, if you try and insert before T1 commits, it will fail, because the file still exists, blocking the insert.  That is optimistic locking in action, because if T1 were to rollback, we'd have two versions of the object (the original now un-deleted one and newly inserted one).

What happens if a second transaction reads indices but those are changed due to commit/rollback from the first transaction, before the second transaction gets around to reading the actual object?  It will at least look for a version that matches its transaction. But there certainly is a danger that it will read an object version that doesn't actually match the select predicate. For that reason, reads should also double check that the field still matches.

What happens if a second transaction reads an object but it is changed due to commit/rollback from the first transaction, before the second transaction gets around to using the object in a useful way?  It will have selected data that no longer exists or is a different version. That is a danger that all databases have and if your business rules cause you to do something that in the mean time shouldn't happen, you need to resolve that in application logic. That is like the case of selecting multiple on-call doctors and updating all to be off-call, when your business rules say that at least one must remain on-call.  In abstraDB you cannot use a selected object that no longer exists to do an update, because you will get an error because your ETag won't match.


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
