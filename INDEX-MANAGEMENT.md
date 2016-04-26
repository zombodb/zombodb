# Index Management

Index management happens through normal Postgres SQL commands.

## CREATE INDEX

To create a ZomboDB index, use the form:

```sql
CREATE INDEX idxname 
          ON table
       USING zombodb (zdb('table', table.ctid), zdb(table))
        WITH (url='...', 
              shards=N, 
              replicas=N,
              preference='...',
              options='...');
```

The `(zdb('table', ctid), zdb(table))` construct is required and causes the index to be a multi-"column" _functional index_.
The output of the `zdb(table)` function is what is actually indexed -- which is a JSON-formatted version of each row.

The `WITH` settings are:

### Basic Settings
- `url` **required**: The base url of the primary entry point into your Elasticsearch cluster.  For example: `http://192.168.0.75:9200/`.  The value **must** end with a forward slash (`/`).
- `shards` **optional**:  The number of Elasticsearch shards to use.  The default is `5`.  Changing this value requires a `REINDEX` before the new value becomes live.
- `replicas` **optional**:  The number of Elasticsearch replicas to use.  The default is `1`.  Changing this value requires a `REINDEX` before the new value becomes live.  In the case of `replicas`, this is a limition of ZomboDB that may be lifted in the future.

### Advanced Settings
- `options` **optional**:  `options` is a ZomboDB-specific string that allows you to define how this index relates to other indexes.  This is an advanced-use feature and is documented [here](INDEX-OPTIONS.md).
- `shadow` **optional** (mutually exclusive with `url`): The name of an existing ZomboDB index that this index should use, but likely with a different set of options.  This too is an [advanced-use](INDEX-OPTIONS.md) feature.
- `field_lists` **optional**:  Allows for the definition of a fields that, when queried, are dynamically expanded to search a list of other fields.  The syntax for this setting is:  `field_lists='fake_field1=[a, b, c], fake_field2=[d,e,f], ...'`.  This can be useful, for example, for searching all "date" fields at once, or defining a set of fields that represent "names" or "locations".  Note that each field in a list must be of the same underlying Postgres data type.

### Operational Settings
- `preference` **optional**:  The Elasticsearch [search preference](https://www.elastic.co/guide/en/elasticsearch/reference/master/search-request-preference.html) to use.  The default is `null`, meaning no search preference is used.
- `bulk_concurrency` **optional**:  Specifies the maximum number of concurrent HTTP requests, per Postgres backend, to use when making "bulk" changes, which include CREATE INDEX/REINDEX, INSERT, UPDATE, DELETE statements.  The default is `12` and allowed values are in the set `[1..12]`
- `batch_size` **optional**:  Specifies the size, in bytes, for batch POST data.  Affects CREATE INDEX/REINDEX, INSERT, UPDATE, DELETE, and VACUUM statements.  The default is `8388608` bytes (8MB) and allowed values are `[1k..64MB]`.

## DROP INDEX

To drop a ZomboDB index, use Postgres' standard `DROP INDEX` command:

```sql
DROP INDEX idxname;
```

This removes the index from Postgres' system catalogs but does **not** also delete the index from Elasticsearch.  You must do that manually, for example:

```
$ curl -XDELETE http://cluster.ip:9200/db.schema.table.index
```

NOTE:  This may change in the future such that the Elasticsearch search index is deleted.  It's currently unclear which direction is most helpful.

## REINDEX

To reindex an exising ZomboDB index, simply use Postgres' standard [REINDEX](http://www.postgresql.org/docs/9.3/static/sql-reindex.html) command.  All the various forms of `INDEX`, `TABLE`, and `DATABASE` are fully supported.


## ALTER INDEX

You can use Postgres' `ALTER INDEX` command to change any of the `WITH` settings defined above.  For example:

```sql
ALTER INDEX idxname SET (replicas=3);
ALTER INDEX idxname RESET (preference);
```

Currently, a `REINDEX` is required after changing an index setting that would affect the physical structure of the underlying Elasticsearch index.  This includes changing both `shards` and `replicas`, despite Elasticsearch being able to dynamically add/remove replicas.  This may be resolved in a future version.

## ALTER TABLE

The various forms of ALTER TABLE that add/drop columns or change column types are supported.  Note that if added columns have a default value or if the type change isn't directly cast-able, Postgres will automatically rebuild the index, so these operations could take a significant amount of time.

>##### WARNING:  Renaming columns is not supported, and doing so will leave the underlying Elasticsearch index in a state that is inconsistent with Postgres.


## VACUUM

Running a standard `VACUUM` on a table with a ZomboDB index does the minimum amount of work to remove dead rows from the backing Elastisearch index and shoud happen in a reasonable amount of time (depending, of course, on the update frequency).

Running a `VACUUM FULL` on a table with a ZomboDB index, on the otherhand, is functionally equilivant to running a `REINDEX` on the table, which means a `VACUUM FULL` could take a long time to complete.

A [`VACUUM FREEZE`](http://www.postgresql.org/docs/9.4/static/routine-vacuuming.html#VACUUM-FOR-WRAPAROUND) (and autovacuum's anti-wrap-around procedure) will leave ZomboDB indexes in an inconsistent state.  After a `VACUUM FREEZE` on a table, any of its ZomboDB indexes must be `REINDEX`ed.  This is because ZomboDB stores transaction visibility information in the remote Elasticsearch index (the same xmin,xmax,etc values stored on every heap tuple) and Postgres doesn't (yet) provide a way to be notified when that data is changed via VACUUM.
