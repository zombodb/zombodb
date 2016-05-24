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
- `replicas` **optional**:  The number of Elasticsearch replicas to use.  The default is `1` and allowed values are `[0..64]`.  Changing this property requires that you call `zdb_update_mapping(tablename_containg_index)` to push the setting to Elasticsearch.

### Advanced Settings
- `options` **optional**:  `options` is a ZomboDB-specific string that allows you to define how this index relates to other indexes.  This is an advanced-use feature and is documented [here](INDEX-OPTIONS.md).
- `shadow` **optional** (mutually exclusive with `url`): The name of an existing ZomboDB index that this index should use, but likely with a different set of options.  This too is an [advanced-use](INDEX-OPTIONS.md) feature.
- `field_lists` **optional**:  Allows for the definition of a fields that, when queried, are dynamically expanded to search a list of other fields.  The syntax for this setting is:  `field_lists='fake_field1=[a, b, c], fake_field2=[d,e,f], ...'`.  This can be useful, for example, for searching all "date" fields at once, or defining a set of fields that represent "names" or "locations".  Note that each field in a list must be of the same underlying Postgres data type.
- `always_resolve_joins` **optional**:  The default is `false` which allows ZomboDB to directly query a linked index for aggregates and `zdb_estimate_count()` when the query only queries fields from one linked index (see `options` under Advanced Settings below along with the [INDEX-OPTIONS](INDEX-OPTIONS.md) documentation).  If your links/joins are always 1-to-1, this is perfectly safe and quite a performance improvement.  If your links/joins are 1-to-many (or many-to-1) this is not safe and you should set the value to `true`.

### Operational Settings
- `preference` **optional**:  The Elasticsearch [search preference](https://www.elastic.co/guide/en/elasticsearch/reference/master/search-request-preference.html) to use.  The default is `null`, meaning no search preference is used.
- `bulk_concurrency` **optional**:  Specifies the maximum number of concurrent HTTP requests, per Postgres backend, to use when making "batch" changes, which include CREATE INDEX/REINDEX, INSERT, UPDATE, COPY statements.  The default is `12` and allowed values are in the set `[1..1024]`
- `batch_size` **optional**:  Specifies the size, in bytes, for batch POST data.  Affects CREATE INDEX/REINDEX, INSERT, UPDATE, COPY, and VACUUM statements.  The default is `8388608` bytes (8MB) and allowed values are `[1k..1Gb]`.  Note that ZomboDB will potentially consume `batch_size * bulk_concurrency` bytes of memory during any given statement, **per backend connection**.
- `refresh_interval` **optional**:  This setting directly relates to Elasticsearch's [`index.refresh_interval`](https://www.elastic.co/guide/en/elasticsearch/reference/1.7/setup-configuration.html#configuration-index-settings).  The default value is `-1`, meaning ZomboDB will control index refreshes such that modified rows are always immediately visible.  If a time delay is okay, change this setting to something like `5s` for five seconds.  This can help improve single-record INSERT/UPDATE performance at the expense of when the rows are actually searchable.  Changing this property requires that you call `zdb_update_mapping(tablename_containg_index)` to push the setting to Elasticsearch.    
- `ignore_visibility` **optional**:  Controls, on a per-index level, if queries that require row visibility information to be MVCC-correct should honor it.  The default for `ignore_visibility` is `false`, meaning all queries are MVCC-correct.  Set this to `true` if you don't need exact values for aggregates and `zdb_estimate_count()`.

### Per-session Settings

- `zombodb.batch_mode`:  This is a boolean "GUC" that controls how ZomboDB sends index changes to Elasticsearch.  The default for `zombodb.batch_mode` is `false` which means that ZomboDB sends index changes to ES on a per-statement level and flushes the remote Elasticsearch index at the end of each statement.  Setting this to `true` will cause ZomboDB to batch index changes through the life of the transaction, and the final set of changes won't be available for search until `COMMIT`.  When set to `true`, ZomboDB delays flushing the index until transaction `COMMIT`.  This can be changed interactively by issuing `SET zombodb.batch_mode = true;`
- `zombodb.ignore_visibility`:  This is a boolean "GUC" that controls if ZomboDB will honor MVCC visibility rules.  The default is `false` meaning it will, but you can `SET zombodb.ignore_visibility = true;` if you don't mind having dead/invisible rows counted in aggregates and `zdb_estimate_count()`.  This is similiar to the index-level setting of the same name, but can be controlled per session.

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

To reindex an exising ZomboDB index, simply use Postgres' standard [REINDEX](http://www.postgresql.org/docs/9.5/static/sql-reindex.html) command.  All the various forms of `INDEX`, `TABLE`, and `DATABASE` are fully supported.


## ALTER INDEX

You can use Postgres' `ALTER INDEX` command to change any of the `WITH` settings defined above.  For example:

```sql
ALTER INDEX idxname SET (replicas=3);
ALTER INDEX idxname RESET (preference);
```

Chagning non-structure settings such as `replicas` and `refresh_interval` do not require a reindex, but do require you call `zdb_update_mapping(tablename)` to push these changes to Elasticsearch.

However, a `REINDEX` is required after changing an index setting that would affect the physical structure of the underlying Elasticsearch index, specifically `shards`.  This is a limitation of Elasticsearch in that shard count is fixed at index creation time.


## ALTER TABLE

The various forms of ALTER TABLE that add/drop columns or change column types are supported.  Note that if added columns have a default value or if the type change isn't directly cast-able, Postgres will automatically rebuild the index, so these operations could take a significant amount of time.

####WARNING:  

Renaming columns is not supported. What will happen is that newly INSERTed/UPDATEd rows will use the new column name but existing rows will use the old name, making searching difficult.  

If you need rename a column, you'll need to manually issue a `REINDEX`.

Perhaps in the future, ZomboDB can transparently alias fields to avoid this situation, but at present, it's not the case.


## VACUUM

Running a standard `VACUUM` on a table with a ZomboDB index does the minimum amount of work to remove dead rows from the backing Elastisearch index and shoud happen in a reasonable amount of time (depending, of course, on the update frequency).

Running a `VACUUM FULL` on a table with a ZomboDB index, on the otherhand, is functionally equilivant to running a `REINDEX` on the table, which means a `VACUUM FULL` could take a long time to complete.

For tables with ZomboDB indexes that are frequently UPDATEd, it's important that autovacuum be configured to be as aggressive as your I/O subsystem can afford.  This is because certain types of ZomboDB queries need to build an "invisibility map", which necessitates looking at every heap page that is not known to be all-visibile.

