# Index Management

ZomboDB index management happens through standard Postgres DDL statements such as `CREATE INDEX`, `ALTER INDEX`, and
`DROP INDEX`. ZomboDB also exposes a number of index-level options that can be set to affect things like number of
shards, replicas, etc.

## CREATE INDEX

The form for creating ZomboDB indices is:

```sql
CREATE INDEX index_name 
          ON table_name 
       USING zombodb ((table_name.*)) 
        WITH (...)
```

(where the options for `WITH` are detailed below)

ZomboDB generates a UUID to use as the backing Elasticsearch index name, but also assigns an alias in the form of
`database_name.schema_name.table_name.index_name-index_oid`. "index_oid" is the Postgres catalog id for the index from
the "pg_class" system catalog table.

The alias is meant to be a human-readable name that you can use with external tools like Kibana or even curl.

## ALTER INDEX

The various Index Options supported by ZomboDB can be changed using Postgres `ALTER INDEX` statement. They can be
changed to new values or reset to their defaults.

For example:

```sql
ALTER INDEX index_name SET (replicas=2)
```

## DROP INDEX/TABLE/SCHEMA/DATABASE

When you drop a Postgres object that contains a ZomboDB index, the corresponding Elasticsearch is also deleted.

`DROP` statements are transaction safe and don't delete the backing Elasticsearch index until the controlling
transaction commits.

Note that `DROP DATABASE` can't delete its corresponding Elasticsearch indices as there's no way for ZomboDB to receive
a notification that a database is being dropped.

## WITH (...) Options

All of the below options can be set during `CREATE INDEX` and most of them can be changed with `ALTER INDEX`. Those that
cannot be altered are noted.

### Required Options

#### `url`

```
Type: string
Default: zdb.default_elasticsearch_url
```

The Elasticsearch Cluster URL for the index. This option is required, but can be omitted if the `postgresql.conf`
setting `zdb.default_elasticsearch_url` is set. This option can be changed with `ALTER INDEX`, but you must be a
Postgres superuser to do so.

The value must end with a forward slash (`/`).

### Elasticsearch Options

#### `shards`

```
Type: integer
Default: 5
Range: [1, 32768]
```

The number of shards Elasticsearch should create for the index. This option can be changed with `ALTER INDEX` but you
must issue a `REINDEX INDEX` before the change will take effect.

#### `replicas`

```
Type: integer
Default: zdb.default_replicas
```

This controls the number of Elasticsearch index replicas. The default is the value of the `zdb.default_replicas` GUC,
which itself defaults to zero. Changes to this value via `ALTER INDEX` take effect immediately.

#### `alias`

```
Type: string
Default: "database.schema.table.index-index_oid"
```

You can set an alias to use to identify an index from external tools. This is for user convenience only. Changes via
`ALTER INDEX` take effect immediately.

Normal SELECT statements are executed in Elasticsearch directly against the named index. Aggregate functions such as
`zdb.count()` and `zdb.terms()` use the alias, however.

In cases where you're using ZomboDB indices on inherited tables or on partition tables, it is suggested you assigned the
**same** alias name to all tables in the hierarchy so that aggregate functions will run across all the tables involved.

#### `refresh_interval`

```
Type: string
Default: "-1"
```

This option specifies how frequently Elasticsearch should refresh the index to make changes visible to searches. By
default, this is set to `-1` because ZomboDB wants to control refreshes itself so that it can maintain proper MVCC
visibility results. It is not recommented that you change this setting unless you're okay with search results being
inconsistent with what Postgres expects. Changes via `ALTER INDEX` take effect immediately.

#### `type_name`

```
Type: string
Default: "_doc"
```

This is the Elasticsearch index type into which documents are mapped. The default, "\_doc" is compatible with
Elasticsearch v5 and v6. There should be no need to change this setting. Note that it can only be set during
`CREATE INDEX`.

#### `translog_durability`

```
Type: string
Default: "request"
Valid values: "request", "async"
```

Whether or not to fsync and commit the translog after every index, delete, update, or bulk request. This setting accepts
the following parameters:

- request: (default) fsync and commit after every request. In the event of hardware failure, all acknowledged writes
  will already have been committed to disk.
- async: fsync and commit in the background every sync_interval. In the event of a failure, all acknowledged writes
  since the last automatic commit will be discarded.

See: https://www.elastic.co/guide/en/elasticsearch/reference/7.x/index-modules-translog.html#\_translog_settings

#### `max_result_window`

```
Tyoe: integer
Default: 10000
Range: [1, INT_32_MAX]
```

The maximum number of docs ZomboDB will retrieve from Elasticsearch in a single scroll request.

See: https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#index-max-result-window

#### `total_fields_limit`

```
Type: integer
Default: 1000
Range: [1, INT_32_MAX]
```

The maximum number of fields in an index. Field and object mappings, as well as field aliases count towards this limit.
The default value is 1000.

See: https://www.elastic.co/guide/en/elasticsearch/reference/master/mapping-settings-limit.html

#### `max_terms_count`

```
Type: integer
Default: 65535
Range: [1, INT_32_MAX]
```

The maximum number of terms that can be used in Terms Query.

Increasing this limit might be necessary for performing large [cross-index joins](CROSS-INDEX-JOINS.md) when the ZomboDB
Search Accelerator is not installed.

https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#index-max-terms-count

#### `max_analyze_token_count`

```
Type: integer
Default: 10000
Range: [1, INT_32_MAX]
```

The maximum number of tokens that the `_analyze` API will generate for a single request. Typically, this is used to
enable custom highlighting of very large documents.

https://www.elastic.co/guide/en/elasticsearch/reference/master/indices-analyze.html#tokens-limit-settings

### Network Options

#### `bulk_concurrency`

```
Type: integer
Default: 12
Range: [1, 1024]
```

When synchronizing changes to Elasticsearch, ZomboDB does this by multiplexing HTTP(S) requests using libcurl. This
setting controls the number of concurrent requests. ZomboDB also logs how many active concurrent requests it's managing
during writes to Elasticsearch. You can use that value to ensure you're not overloading your Elasticsearch cluster.
Changes via `ALTER INDEX` take effect immediately.

#### `batch_size`

```
Type: integer (in bytes)
Default: 8388608
Range: [1024, (INT_MAX/2)-1]
```

When synchronizing changes to Elasticsearch, ZomboDB does this by batching them together into chunks of `batch_size`.
The default of 8mb is a sensible default, but can be changed in conjunction with `bulk_concurrency` to improve overall
write performance. Changes via `ALTER INDEX` take effect immediately.

#### `compression_level`

```
Type: integer
Default: 1
Range: [0, 9]
```

Sets the HTTP(s) transport (and request body) deflate compression level. Over slow networks, it may make sense to set
this to a higher value. Setting to zero turns off all compression. Changes via `ALTER INDEX` take effect immediately.

### Nested Object Mapping Options

#### `nested_fields_limit`

```
Type: integer
Default: 1000
Range: [1, INT_32_MAX]
```

The maximum number of distinct nested mappings in an index. The nested type should only be used in special cases, when
arrays of objects need to be queried independently of each other. To safeguard against poorly designed mappings, this
setting limits the number of unique nested types per index.

See: https://www.elastic.co/guide/en/elasticsearch/reference/master/mapping-settings-limit.html

#### `nested_objects_limit`

```
Type: integer
Default: 10000
Range: [1, INT_32_MAX]
```

The maximum number of nested JSON objects that a single document can contain across all nested types. This limit helps
to prevent out of memory errors when a document contains too many nested objects.

See: https://www.elastic.co/guide/en/elasticsearch/reference/master/mapping-settings-limit.html#mapping-settings-limit

#### `nested_object_date_detection`

```
Type: bool
Default: false
```

If `nested_object_date_detection` is enabled (default is false), then new string fields in nested objects (fields of
type 'json' or 'jsonb') are checked to see whether their contents match any of the date patterns specified in
dynamic_date_formats. If a match is found, a new date field is added with the corresponding format.

The default value for dynamic_date_formats is:

```json
 [ "strict_date_optional_time","yyyy/MM/dd HH:mm:ss Z||yyyy/MM/dd Z"]
```

See: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html#date-detection

#### `nested_object_numeric_detection`

```
Type: bool
Default: true
```

While JSON has support for native floating point and integer data types, some applications or languages may sometimes
render numbers as strings. Usually the correct solution is to map these fields explicitly, but numeric detection (which
is disabled by default) can be enabled to do this automatically.

See: https://www.elastic.co/guide/en/elasticsearch/reference/current/dynamic-field-mapping.html#numeric-detection

#### `nested_object_text_mapping`

```
Type: String (as JSON)
Default: {
           "type": "keyword",
           "ignore_above": 10922,
           "normalizer": "lowercase",
           "copy_to": "zdb_all"
         }
```

By default, ZomboDB will map "string" properties found in nested objects (fields of type 'json' or 'jsonb') using the
above type mapping -- they'll be indexed as full-value keywords.

You can override this at `CREATE INDEX` time.

### Advanced Options

#### `include_source`

```
Type: bool, default 'true'
```

Should the source of each row be included in the _source field of the document within Elasticsearch?

Turning this off is useful for reducing the size of the documents, but it's not recommended for production use.  The
primary problem with this is that it makes it impossible to properly UPDATE/DELETE rows.  This is for experts only.

#### `options`

```
Type: comma-separated String of index link definitions
```

`options` is a ZomboDB-specific string that allows you to define how this index relates to other indexes. This is an
advanced-use feature and is documented [here](CROSS-INDEX-JOINS.md).

Changes via `ALTER INDEX` take effect immediately.

#### `field_lists`

```
Type: comma-separated String
```

Allows to define lists fields that, when queried, are dynamically expanded to search their defined list of other fields.
The syntax for this setting is:

```
field_lists='fake_field1=[a, b, c], fake_field2=[d,e,f], ...'
```

This can be useful, for example, for searching all "date" fields at once, or defining a set of fields that represent
"names" or "locations". Note that each field in a list must be of the same underlying Postgres data type.

Changes via `ALTER INDEX` take effect immediately.
