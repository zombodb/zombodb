## Support for Citus Community/Enterprise 6.0+

Thanks to some [cooperation](https://github.com/citusdata/citus/pull/773) with the very smart folks at [Citus](https://citusdata.com/), ZomboDB and Citus Community/Enterprise can now be used together.  ZomboDB is not available on the Citus Cloud offering.

When using ZomboDB with Citus, there are some differences and certain ZomboDB features don't work, but basic index creation and searching do.

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
        USING zombodb (zdb(table.tableoid, ctid), zdb(table)) 
        WITH (url='http://localhost:9200/');

SELECT * FROM table WHERE zdb(table.tableoid, ctid) ==> 'text query here';
```

You can actually use the second form in non-Citus installations too.


### Elasticsearch Index Differences

Without Citus, ZomboDB creates one index in Elasticsearch (per `CREATE INDEX` statement).  With Citus, however, an Elasticsearch index will be created for each shard of a distributed table.

You only need to create the index on the master Citus node, and Citus will take care of distributing it across all the table shards.

It's important to note that each shard inherits the `WITH (...)` settings of the `CREATE INDEX` statement, so each Citus worker node will need access to the server behind ZomboDB's `url='...'` option.

The Elasticsearch indexes that are created are named with the Citus `shardId`, just as the actual shard tables do, so they'll be relateable via tools like Marvel.


### ZomboDB features that don't work

There's quite a large list of ZomboDB features that don't (yet) work under Citus.

Rather than list them individually, the answer is basically any SQL-level function that starts with `zdb_` and executes a query.  This includes functions like `zdb_estimate_count()` and `zdb_tally()`.

The hope is that these ZomboDB features will be supported in the future.


### ZomboDB features that do work

This is limited to `CREATE INDEX` and `SELECT ... WHERE zdb(table.tableoid, ctid) ==> 'text query'`.

When creating a new index via `CREATE INDEX`, the statement is not run in parallel.  Citus will create each index for each shard on each worker in serial.

That said, `SELECT` statements are run in parallel.
