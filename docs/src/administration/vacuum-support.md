# VACUUM

ZomboDB indices fully interoperate with Postgres (auto)VACUUM facilities, and keeping ZomboDB indicies vacuumed is
important for maintaining a high level of search performance.

Because ZomboDB stores row-level visibility information in its indices, ZomboDB's vacuum process is quite a bit
different than standard index types like btree.

ZomboDB's approach is to:

1. Find all docs with a known-to-be aborted `xmin`. These represent rows where the inserting/updating transaction
   aborted. They can be deleted
1. Find all docs with a known-to-be committed `xmax`. These represent deleted rows or old versions of updated rows from
   a committed transaction. They can be deleted.
1. Find all docs with a known-to-be aborted `xmax`. These represent rows where the updating/deleting transaction
   aborted. These rows can have their `xmax` reset to `null`
1. From ZDB's aborted transaction id list, determine which are not referenced as either an xmin or xmax. These
   individual xid values can be removed from the list as they're not referenced anymore.

In all cases, the evaluation of "known-to-be" means that the transaction id is older than the "oldest xmin" that
Postgres determines. This means the xid's state is known to all past, present, and future transactions.

Additionally, in cases #1 and #2 ZomboDB needs to perform a "scripted delete" against Elasticsearch whereby it only
deletes the doc if, in the case of #1, the doc's current `xmin` matches what we expected it to be, and in the case of #2
and #3, if the doc's current `xmax` matches what we expect it to be. This is because Postgres could decide to reuse
those heap tuple slots between when ZomboDB's vacuum process identifies that row and when it tries to delete it.

## VACUUM Considerations

The first thing to consider is that a `VACUUM FULL` will also reindex any indicies attached to the table, including
ZomboDB indices. As such, a `VACUUM FULL` could take a very long time.

A normal `VACUUM` will simply do the work outlined above.

A `VACUUM FREEZE` will adjust xmin/xmax values on the heap but not change anything in the ZomboDB indices. This is
actually okay as ZomboDB stores epoch-encoded 64bit transaction ids that aren't subject to wraparound issues that
`VACUUM FREEZE` is designed to prevent.
