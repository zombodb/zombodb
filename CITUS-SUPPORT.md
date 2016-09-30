## Support for Citus Community/Enterprise 6.0+

Thanks to some [cooperation](https://github.com/citusdata/citus/pull/773) with the very smart folks at [Citus](https://citusdata.com/), ZomboDB and Citus Community/Enterprise can now be used together.  ZomboDB is not available on the Citus Cloud offering.

When using ZomboDB with Citus, there are some syntax differences when creating and querying indexes.  Additionally, some differences exist around how ZomboDB resolves MVCC visibility for aggregate and count estimation functions.

Note that Citus won't contain support for ZomboDB until Citus 6.0 is released.

### Syntax Differences

Rather than using `regclass`-typed strings to reference tables, ZomboDB+Citus requires that the `tableoid` system column be used.  This is true for both `CREATE INDEX` statements and `SELECT` statements.

Standard ZomboDB:

```sql
CREATE INDEX idx ON table 
        USING zombodb (zdb('table', ctid), zdb(table)) 
        WITH (url='http://localhost:9200/');

SELECT * FROM table WHERE zdb('table', ctid) ==> 'text query here';
```

ZomboDB+Citus:

```sql
CREATE INDEX idx ON table 
        USING zombodb (zdb(tableoid, ctid), zdb(table.*)) 
        WITH (url='http://localhost:9200/');

SELECT * FROM table WHERE zdb(tableoid, ctid) ==> 'text query here';
```

You can actually use the second form in non-Citus installations too.

### Elasticsearch Index Differences

Without Citus, ZomboDB creates one index in Elasticsearch (per `CREATE INDEX` statement).  With Citus, however, an Elasticsearch index will be created for each shard of a distributed table.

You only need to create the index on the master Citus node, and Citus will take care of distributing it across all the table shards.

It's important to note that each shard inherits the `WITH (...)` settings of the `CREATE INDEX` statement, so each Citus worker node will need access to the server behind ZomboDB's `url='...'` option.

The Elasticsearch indexes that are created are named with the Citus `shardId`, just as the actual shard tables are, so they'll be relateable via tools like Marvel.

If you want to be able to use ZomboDB's aggregation and count estimation SQL functions with Citus, you also need to set an `alias` option on the index.  The value can be whatever you like (I suggest just using the table name), but it should be unique across your Elasticsearch cluster.

Behind the scenes, ZomboDB will associate each Citus shard index with that alias in Elasticsearch and will query that alias when ZomboDB specifically needs access to the "entire index" to answer a query.  In general, this is limited to only aggregate and count estimation functions.


### MVCC Row Visibility Differences

Without Citus, ZomboDB is capable of resolving MVCC row visibility inside Elasticsearch because ZomboDB tracks certain bits of transaction metadata (mainly, the transaction id that created a row).

ZomboDB only needs to do this for its various aggregation and count estimation functions, such as `zdb_tally()` and `zdb_estimate_count()`.

With a multi-node Citus cluster, each worker node has its own concept of a "current transaction id", and as such, ZomboDB can't use this to resolve visibility, because a query like `SELECT zdb_estimate_count('table', 'query')` is run on the Citus master node, which most likely has a different "current transaction id" than any of the workers in the cluster.

That said, you can tell ZomboDB to just ignore visibility entirely, and then functions like `zdb_tally()` and `zdb_estimate_count()` will work.  Their results, however, will include rows that are a) live, b) dead, c) uncommitted, d) aborted.

To tell ZomboDB to ignore visibility, set the Postgres setting `zombodb.ignore_visibility` to `true`.  You can do this in an active session:  `SET zombodb.ignore_visibility TO true;`, or you can set it in the Citus master node's `postgresql.conf`.

Note that normal `SELECT ... WHERE zdb(tableoid, ctid) ==> 'query';` statements will always honor proper MVCC row visibility rules because in this case, each Citus worker is calculating visibility while the query is being evaluated.

### CREATE INDEX and SELECT

When creating a new index via `CREATE INDEX`, the statement is not run in parallel.  Citus will create each index for each shard on each worker in serial.

That said, `SELECT` statements are run in parallel.
