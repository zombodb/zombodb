# SQL Functions

ZomboDB provides a number of utility and helper SQL functions.

```sql
FUNCTION zdb.version() RETURNS text
```

Returns the currently-installed version of ZomboDB.


---

```sql
FUNCTION zdb.request(
	index regclass, 
	endpoint text, 
	method text DEFAULT 'GET', 
	post_data text DEFAULT NULL) 
RETURNS text
```

Make an arbitrary REST request to the Elasticsearch cluster hosting the specified index.

If the `endpoint` argument begins with a forward slash (`/`) the request is executed at the root of the Elasticsearch cluster.  Otherwise, the request is executed relative to the specified index.

For example, this returns the Elasticsearch cluster information:

```sql
SELECT zdb.request('idxproducts', '/');
                            request                             
----------------------------------------------------------------
 {                                                             +
   "name" : "emac16.lan",                                      +
   "cluster_name" : "elasticsearch",                           +
   "cluster_uuid" : "HPxSF2doQy-KHFfFKFPEZQ",                  +
   "version" : {                                               +
     "number" : "7.9.0",                                       +
     "build_flavor" : "default",                               +
     "build_type" : "tar",                                     +
     "build_hash" : "a479a2a7fce0389512d6a9361301708b92dff667",+
     "build_date" : "2020-08-11T21:36:48.204330Z",             +
     "build_snapshot" : false,                                 +
     "lucene_version" : "8.6.0",                               +
     "minimum_wire_compatibility_version" : "6.8.0",           +
     "minimum_index_compatibility_version" : "6.0.0-beta1"     +
   },                                                          +
   "tagline" : "You Know, for Search"                          +
 }                                                             +
 
(1 row)
```

Whereas this returns the settings for the specified index:

```sql
 SELECT zdb.request('idxproducts', '_settings');
                                    request                                    
-------------------------------------------------------------------------------
 {                                                                            +
     "19524866.2200.19540060.19540070-882296036": {                           +
         "settings": {                                                        +
             "index": {                                                       +
                 "uuid": "Nw8D3ymUT9mbTCLTgBgMLA",                            +
                 "query": {                                                   +
                     "default_field": "zdb_all"                               +
                 },                                                           +
                 "version": {                                                 +
                     "created": "5060499"                                     +
                 },                                                           +
                 "analysis": {                                                +
                     "filter": {                                              +
...
```

---

```sql
FUNCTION profile_query(index regclass, query zdbquery) RETURNS json
```

Uses Elasticsearch's [Profile API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-profile.html) to provide detailed timing and execution information about a query.

---

```sql
FUNCTION zdb.determine_index(relation regclass) RETURNS regclass
```

Given a relation oid (either an actual index, a table, or a view) return the `USING zombodb` index that will be used when
querying that relation.

If no index can be determined, `NULL` is returned.

---

```sql
FUNCTION zdb.index_name(index regclass) RETURNS text
```

Returns the ZomboDB-generated Elasticsearch index name for the specified Postgres index.

Example:

```sql
SELECT zdb.index_name('idxproducts');
                index_name                 
-------------------------------------------
 19524866.2200.19540060.19540070-882296036
(1 row)
```

---

```sql
FUNCTION zdb.index_url(index regclass) RETURNS text
```

Returns the url to the Elasticsearch cluster which contains the specified Postgres index.

Example:

```sql
SELECT zdb.index_url('idxproducts');
       index_url        
------------------------
 http://localhost:9200/
```

---

```sql
FUNCTION zdb.index_type_name(index regclass) RETURNS text
```

Returns the Elasticsearch index type name being used.  Unless explicitly set during `CREATE INDEX` this will always return the string `doc`.

Example:

```sql
SELECT zdb.index_type_name('idxproducts');
 index_type_name 
-----------------
 doc
(1 row)
```

---

FUNCTION zdb.index_field_lists(index_relation regclass) RETURNS TABLE ("fieldname" text, "fields" text[])

Returns a resultset describing all the field lists that are defined for the specified index.

Example:

```sql
SELECT * FROM zdb_get_index_field_lists('idxsome_index');
    fieldname     |           fields
------------------+----------------------------
 title_and_author | {title,author}
 hashes           | {sha1,md5}
(2 rows)
```

---


```sql
FUNCTION zdb.index_mapping(index regclass) RETURNS jsonb
```

Returns the full Elasticsearch mapping that ZomboDB generated for the specified Postgres index.  This can be useful for ensuring your custom analyzers and field mappings are properly defined.

Example:

```sql
SELECT * FROM zdb.index_mapping('idxproducts');
                                                      index_mapping                                                       
--------------------------------------------------------------------------------------------------------------------------
 {                                                                                                                       +
         "mappings": {                                                                                                   +
             "doc": {                                                                                                    +
                 "_all": {                                                                                               +
                     "enabled": false                                                                                    +
                 },                                                                                                      +
                 "properties": {                                                                                         +
                     "id": {                                                                                             +
                         "type": "long"                                                                                  +
                     },                                                                                                  +
                     "name": {                                                                                           +
                         "type": "text",                                                                                 +
                         "copy_to": [                                                                                    +
                             "zdb_all"                                                                                   +
                         ],                                                                                              +
                         "analyzer": "zdb_standard",                                                                     +
                         "fielddata": true                                                                               +
                     },                                                                                                  +
                     "price": {                                                                                          +
                         "type": "long"                                                                                  +
                     },                                                                                                  +
                     "zdb_all": {                                                                                        +
                         "type": "text",                                                                                 +
                         "analyzer": "zdb_all_analyzer"                                                                  +
                     },                                                                                                  +
                     "keywords": {                                                                                       +
                         "type": "keyword",                                                                              +
                         "copy_to": [                                                                                    +
                             "zdb_all"                                                                                   +
                         ],                                                                                              +
                         "normalizer": "lowercase",                                                                      +
                         "ignore_above": 10922                                                                           +
                     },                                                                                                  +
                     "zdb_cmax": {                                                                                       +
                         "type": "integer"                                                                               +
                     },                                                                                                  +
                     "zdb_cmin": {                                                                                       +
                         "type": "integer"                                                                               +
                     },                                                                                                  +
                     "zdb_ctid": {                                                                                       +
                         "type": "long"                                                                                  +
                     },                                                                                                  +
                     "zdb_xmax": {                                                                                       +
                         "type": "long"                                                                                  +
                     },                                                                                                  +
                     "zdb_xmin": {                                                                                       +
                         "type": "long"                                                                                  +
                     },                                                                                                  +
                     "discontinued": {                                                                                   +
                         "type": "boolean"                                                                               +
                     },                                                                                                  +
                     "short_summary": {                                                                                  +
                         "type": "text",                                                                                 +
                         "copy_to": [                                                                                    +
                             "zdb_all"                                                                                   +
                         ],                                                                                              +
                         "analyzer": "zdb_standard",                                                                     +
                         "fielddata": true                                                                               +
                     },                                                                                                  +
                     "inventory_count": {                                                                                +
                         "type": "integer"                                                                               +
                     },                                                                                                  +
                     "long_description": {                                                                               +
                         "type": "text",                                                                                 +
                         "copy_to": [                                                                                    +
                             "zdb_all"                                                                                   +
                         ],                                                                                              +
                         "analyzer": "zdb_standard"                                                                      +
                     },                                                                                                  +
                     "zdb_aborted_xids": {                                                                               +
                         "type": "long"                                                                                  +
                     },                                                                                                  +
                     "availability_date": {                                                                              +
                         "type": "date",                                                                                 +
                         "copy_to": [                                                                                    +
                             "zdb_all"                                                                                   +
                         ]                                                                                               +
                     }                                                                                                   +
                 },                                                                                                      +
                 "dynamic_templates": [                                                                                  +
                     {                                                                                                   +
                         "strings": {                                                                                    +
                             "mapping": {                                                                                +
                                 "type": "keyword",                                                                      +
                                 "copy_to": "zdb_all",                                                                   +
                                 "normalizer": "lowercase",                                                              +
                                 "ignore_above": 10922                                                                   +
                             },                                                                                          +
                             "match_mapping_type": "string"                                                              +
                         }                                                                                               +
                     },                                                                                                  +
                     {                                                                                                   +
                         "dates_times": {                                                                                +
                             "mapping": {                                                                                +
                                 "type": "date",                                                                         +
                                 "format": "strict_date_optional_time||epoch_millis||HH:mm:ss.SSSSSS||HH:mm:ss.SSSSSSZZ",+
                                 "copy_to": "zdb_all"                                                                    +
                             },                                                                                          +
                             "match_mapping_type": "date"                                                                +
                         }                                                                                               +
                     }                                                                                                   +
                 ]                                                                                                       +
             }                                                                                                           +
         }                                                                                                               +
     }
(1 row)
```

---


```sql
FUNCTION zdb.field_mapping(index_relation regclass, field_name text) RETURNS json
```

Returns the Elasticsearch field mapping definition for the specified field.  In the event the specified index has
index links defined this will traverse those links to find the specified field.

Example:

```sql
select zdb.field_mapping('idxevents', 'event_type');
                                         field_mapping                                         
-----------------------------------------------------------------------------------------------
 {"type": "keyword", "copy_to": ["zdb_all"], "normalizer": "lowercase", "ignore_above": 10922}
(1 row)

```
