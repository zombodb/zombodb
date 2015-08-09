# Index Management

Index management happens through normal Postgres SQL commands.

## CREATE INDEX

To create a ZomboDB index, use the form:

```
CREATE INDEX idxname 
          ON table
       USING zombodb (zdb(table))
        WITH (url='...', 
              shards=N, 
              replicas=N,
              preference='...',
              options='...');
```

The `zdb(table)` construct is required and causes the index to be a _functional index_.  The output of the `zdb()` function is what is actually indexed -- which is a JSON-formatting version of each row.

The `WITH` settings are:

### Basic Settings
- `url` **required**: The base url of the primary entry point into your Elasticsearch cluster.  For example: `http://192.168.0.75:9200/`.
- `shards` **optional**:  The number of Elasticsearch shards to use.  The default is `5`.  Changing this value requires a `REINDEX` before the new value becomes live.
- `replicas` **optional**:  The number of Elasticsearch replicas to use.  The default is `1`.  Changing this value requires a `REINDEX` before the new value becomes live.  In the case of `replicas`, this is a limition of ZomboDB that may be lifted in the future.
- `preference` **optional**:  The Elasticsearch [search preference](https://www.elastic.co/guide/en/elasticsearch/reference/master/search-request-preference.html) to use.  The default is `null`, meaning no search preference is used.

### Advanced Settings
- `options` **optional**:  `options` is a ZomboDB-specific string that allows you to define how this index relates to other indexes.  This is an advanced-use feature and is documented [here](INDEX-OPTIONS.md).
- `shadow` **optional** (mutually exclusive with `url`): The name of an existing ZomboDB index that this index should use, but likely with a different set of options.  This too is an [advanced-use](INDEX-OPTIONS.md) feature.


## DROP INDEX

To drop a ZomboDB index, use Postgres' standard `DROP INDEX` command:

```
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

```
ALTER INDEX idxname SET (replicas=3);
ALTER INDEX idxname RESET (preference);
```


## ALTER TABLE

The various forms of ALTER TABLE that add/drop columns or change column types are supported.  Note that if added columns have a default value or if the type change isn't directly cast-able, Postgres will automatically rebuild the index, so these operations could take a significant amount of time.

>##### WARNING:  Renaming columns is not supported, and doing so will leave the underlying Elasticsearch index in a state that is inconsistent with Postgres.


## VACUUM

Running a standard `VACUUM` on a table with a ZomboDB index does the minimum amount of work to remove dead rows from the backing Elastisearch index and shoud happen in a reasonable amount of time (depending, of course, on the update frequency).

Running a `VACUUM FULL` on a table with a ZomboDB index, on the otherhand, is functionally equilivant to running a `REINDEX` on the table, which means a `VACUUM FULL` could take a long time to complete.