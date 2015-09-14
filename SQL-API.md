# ZomboDB SQL API

While the heart of ZomboDB is an index, the extension provides a number of SQL-callable functions for asking out-of-band questions of the underlying Elasticsearch index.


## Custom DOMAIN types

These custom domains are to be used in user tables as data types when you require text parsing and analysis for a given field.

#### ```DOMAIN phrase AS text```

>This domain indicates to ZomboDB that the corresponding field should be analyzed in Elasticsearch

#### ```DOMAIN fulltext AS text```

>Currently has exact same meaning as the ```phrase``` domain.  This might change in the future

#### ```DOMAIN phrase_array AS text[]```

>Similar to ```phrase``` but each array element will be analyzed.  It's spelled this way because Postgres does not currently support arrays over DOMAIN objects, ie ```phrase[]```.


## Custom Operators

#### ```OPERATOR ==> (LEFTARG=json, RIGHTARG=text)```

>This operator is ZomboDB's "full text query" operator.  It can only be used in the context of an IndexScan and is also used in combination with the ```zdb(record)``` function (see below).
>
>Example:
>
>```SELECT * FROM table WHERE zdb(table) ==> 'full text query';```


## SQL Functions

#### ```FUNCTION zdb(r record) RETURNS json```

> ```r```: a record reference
> 
>This function is required when creating "zombodb" indexes and when performing full text queries.
>
>returns a JSON-ified version of input record.
>
>Examples:
>
>```CREATE INDEX idxfoo ON table USING zombodb (zdb(table)) WITH (...);```  
>```SELECT * FROM table WHERE zdb(table) ==> '...';```

#### ```FUNCTION zdb_actual_index_record_count(table_name regclass, type text) RETURNS bigint```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```type```: the Elasticsearch type to inspect, one of 'xact' or 'data'
> 
> returns the actual number of documents contained in Elasticsearch for the index on the specified table name.
> 
> Example:
> 
> ```SELECT * FROM zdb_actual_index_record_count('table', 'data');```

#### ```FUNCTION zdb_arbitrary_aggregate(table_name regclass, aggregate_query json, query text) RETURNS json```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```aggregate_query```: specialized ZomboDB-specific syntax to chain together one or more ZomboDB-supported aggregation types (terms, significant terms, suggestions, extended statistics)  
> ```query```: a full text query
> 
> returns the Elasticsearch-created JSON results.  The data returned is MVCC-safe.
> 
> This function is primary used for building and returning nested aggregation queries.  Currently, only the three aggregation types ZomboDB supports can be used.
> 
> The syntax for the `aggregate_query` argument follows the form:
> 
> ```
> #tally(fieldname, stem, max_terms, term_order [, another aggregate])
> ```
> 
> or
> 
> ```
> #range(fieldname, '<ES "range" specification JSON>' [, another aggregate])
> ```
> 
> or
> 
> ```
> #significant_terms(fieldname, stem, max_terms [, another aggregate])
> ```
> 
> or
> 
> ```
> #extended_stats(fieldname)
> ```
> 
> or
> 
> ```
> #suggest(fieldname, base_term, max_terms)
> ```
> 
> Then then they can be chained together to form complex, nested aggregations.  For example, using the `products` table from the [TUTORIAL](TUTORIAL.md), to break down the products by availability month and keyword:
> 
> Example:
> 
> ```
> tutorial=# SELECT * FROM zdb_arbitrary_aggregate('products', '#tally(availability_date, month, 5000, term, #tally(keywords, ''^.*'', 5000, term))', ''); 
                                                                                                                                                                                                                                                                                                                                                                             zdb_arbitrary_aggregate                                                                                                                                                                                                                                                                                                                                                                             
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 {"missing":{"doc_count":0},"availability_date":{"buckets":[{"key_as_string":"2015-07","key":1435708800000,"doc_count":1,"keywords":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"box","doc_count":1},{"key":"negative space","doc_count":1},{"key":"square","doc_count":1},{"key":"wooden","doc_count":1}]}},{"key_as_string":"2015-08","key":1438387200000,"doc_count":3,"keywords":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,"buckets":[{"key":"alexander graham bell","doc_count":1},{"key":"baseball","doc_count":1},{"key":"communication","doc_count":1},{"key":"magical","doc_count":1},{"key":"primitive","doc_count":1},{"key":"round","doc_count":2},{"key":"sports","doc_count":1},{"key":"widget","doc_count":1}]}}]}}
>```
>
>The response is a JSON blob because it's quite difficult to project an arbitrary nested structure into a resultset with SQL.  The intent is that decoding of the response would be application-specific.

#### ```FUNCTION zdb_describe_nested_object(table_name regclass, fieldname text) RETURNS json```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```fieldname```: a field in table_name that is of type json
> 
>returns the dynamically-created Elasticsearch mapping for the field, which is stored as a "nested object".  Can be useful for understanding how Elasticsearch is managing what might be opaque-to-you nested objects.
> 
> Example:
> 
> ```
> SELECT * FROM zdb_describe_nested_object('table', 'dynamic_customer_data');
> ```

#### ```FUNCTION zdb_determine_index(table_name regclass) RETURNS oid```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index
>
>returns the OID of the underlyig Postgres index that ZomboDB will use when querying the table (or view).  Used internally, but can be useful for debugging.  Cast the result to ```regclass``` for a human-readable index name.
>
>Example:
>
>```
>SELECT zdb_determine_index('products')::regclass;
>
> zdb_determine_index 
>---------------------
> idx_zdb_products
>```

#### ```FUNCTION zdb_estimate_count(table_name regclass, query text) RETURNS bigint```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```query```: a full text query
> 
>returns an MVCC-safe count of records that match a full text query.  This is a great alternative to slow-running "SELECT COUNT(*)" queries.
>
>Example:
>
>```
>SELECT * FROM zdb_estimate_count('table', 'subject:meeting');
>```

#### ```FUNCTION zdb_extended_stats(table_name regclass, fieldname text, query text) RETURNS SET OF zdb_extended_stats_response```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```fieldname```: A numeric field in the specified table  
> ```query```: A full text query
> 
> returns the set of Elasticsearch ["extended statistics"](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-extendedstats-aggregation.html) aggregate.
> 
> Example:
> 
>```
> SELECT * FROM zdb_extended_stats('products', 'price', 'telephone or widget');
> 
> count | total | min  | max  |  mean  | sum_of_squares |  variance   | std_deviation 
>-------+-------+------+------+--------+----------------+-------------+---------------
>     2 | 11799 | 1899 | 9900 | 5899.5 |      101616201 | 16004000.25 |        4000.5
>```

#### ```FUNCTION zdb_get_index_mapping(table_name regclass) RETURNS json```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index
> 
> returns the mapping generated for the underlying Elasticsearch index.  The response is the same as hitting Elasticsearch's ```_mapping``` endpoint.
> 
> Example:
> 
> ```
> SELECT zdb_get_index_mapping('products');
> ```
> 
> Result:
> 
> ```
> {"mappings":{"xact":{"_meta":{"primary_key":"id","noxact":false},"date_detection":false,"_all":{"enabled":false},"_field_names":{"enabled":false,"type":"_field_names","index":"no"},"properties":{"_cmax":{"type":"integer","fielddata":{"format":"disabled"}},"_cmin":{"type":"integer","fielddata":{"format":"disabled"}},"_partial":{"type":"boolean","fielddata":{"format":"disabled"}},"_xmax":{"type":"integer","fielddata":{"format":"disabled"}},"_xmax_is_committed":{"type":"boolean","fielddata":{"format":"disabled"}},"_xmin":{"type":"integer","fielddata":{"format":"disabled"}},"_xmin_is_committed":{"type":"boolean","fielddata":{"format":"disabled"}}}},"data":{"_meta":{"primary_key":"id","noxact":false},"date_detection":false,"_all":{"enabled":true,"analyzer":"phrase"},"_parent":{"type":"xact"},"_routing":{"required":true},"_field_names":{"enabled":false,"type":"_field_names","index":"no"},"_source":{"enabled":false},"properties":{"discontinued":{"type":"boolean"},"id":{"type":"long","store":true,"include_in_all":false},"inventory_count":{"type":"integer","store":true,"include_in_all":false},"keywords":{"type":"string","norms":{"enabled":false},"index_options":"docs","analyzer":"exact"},"long_description":{"type":"string","norms":{"enabled":false},"analyzer":"fulltext","fielddata":{"format":"disabled"},"include_in_all":false},"name":{"type":"string","norms":{"enabled":false},"index_options":"docs","analyzer":"exact"},"price":{"type":"long","store":true,"include_in_all":false},"short_summary":{"type":"string","norms":{"enabled":false},"analyzer":"phrase","fielddata":{"format":"disabled"}}}}}}
> ```


#### ```FUNCTION zdb_get_index_name(index_name regclass) RETURNS text```

>```index_name```:  The name of a Postgres index of type "zombodb"
> 
> returns the name of the corresponding Elasticsearch index.
> 
> NB:  this may be renamed in the future to ```zdb_get_es_index_name```
> 
> Example:
> 
> ```
> SELECT zdb_get_index_name('idx_zdb_products');
> 
>            zdb_get_index_name             
>-------------------------------------------
> tutorial.public.products.idx_zdb_products
>```

#### ```FUNCTION zdb_get_url(index_name regclass) RETURNS text```

>```index_name```:  The name of a Postgres index of type "zombodb"
>
> returns the URL of the Elasticsearch cluster containing the index
> 
> Example:
> 
> ```
> SELECT zdb_get_url('idx_zdb_products');
> 
>       zdb_get_url       
>------------------------
> http://localhost:9200/
>```

### ```FUNCTION zdb_highlight(table_name regclass, es_query text, where_clause text) RETURNS SET OF zdb_highlight_response```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```es_query```: a full text query to use for highlighting (not searching)  
> ```where_clause```: a Postgres-compatible where clause to find the documents to highlight
> 
> ```zdb_highlight()``` provides access to ZomboDB's high-speed highlighter.  It is capable of determing highlight positions for multiple documents at once with the same query.  The common usage pattern for this function is to provide an "IN clause" for the ```where_clause``` argument that selects one or more documents by their primary key.
> 
> Note that this function does not return back highlighted text, it instead returns enough term/positional information in order for an external process to apply highlights.  Future versions of ZomboDB may provide this functionality.
> 
> All of ZomboDB's search constructs support highlighting except for the following:
> 
>  - fuzzy terms
>  - "more like this" queries
>  - range queries (ie field:1 /to/ 1000)
> 
> 
> The return type is defined as:
> 
> ```
> CREATE TYPE zdb_highlight_response AS (
> 	"primaryKey" text, 
> 	"fieldName" text, 
> 	"arrayIndex" int4, 
> 	"term" text, 
> 	"type" text, 
> 	"position" int4, 
> 	"startOffset" int8, 
> 	"endOffset" int8, 
> 	"clause" text
> );
```
>
> The resulting highlight matches are guaranteed to be orderd by "primaryKey", "fieldName", "arrayIndex", "position".
> 
> Example:
> 
>```
>SELECT * FROM zdb_highlight('products', 'telephone, widget, base*', 'id IN (1,2,3,4)');
>
 primaryKey |   fieldName   | arrayIndex |   term    |    type    | position | startOffset | endOffset |          clause           
------------+---------------+------------+-----------+------------+----------+-------------+-----------+---------------------------
 1          | keywords      |          1 | widget    | <ALPHANUM> |        1 |           0 |         6 | _all CONTAINS "widget"
 1          | name          |          0 | widget    | <ALPHANUM> |        2 |           8 |        14 | _all CONTAINS "widget"
 1          | short_summary |          0 | widget    | <ALPHANUM> |        2 |           2 |         8 | _all CONTAINS "widget"
 2          | keywords      |          0 | baseball  | <ALPHANUM> |        1 |           0 |         8 | _all CONTAINS "base"
 2          | name          |          0 | baseball  | <ALPHANUM> |        1 |           0 |         8 | _all CONTAINS "base"
 2          | short_summary |          0 | baseball  | <ALPHANUM> |        3 |           7 |        15 | _all CONTAINS "base"
 3          | name          |          0 | telephone | <ALPHANUM> |        1 |           0 |         9 | _all CONTAINS "telephone"

>``` 
>
>NB:  depending on query complexity, the "clause" column can sometimes be incorrect or null


#### ```FUNCTION zdb_significant_terms(table_name regclass, fieldname text, stem text, query text, max_terms bigint) RETURNS SET OF zdb_significant_terms_response```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```fieldname```: The name of a field from which to derive significant terms  
> ```stem```:  a Regular expression by which to filter returned terms   
> ```query```: a full text query  
> ```max_terms```: maximum number of terms to return.  A value of zero means "all terms".
> 
> This function provides direct access to Elasticsearch's ["significant terms"](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significantterms-aggregation.html) aggregation.  The results are MVCC-safe.  Returned terms are forced to upper-case.
> 
> Note:  Fields of type ```fulltext``` are not supported.
> 
> Example:
> 
> ```
> SELECT * FROM zdb_significant_terms('products', 'keywords', '^.*', '', 5000);
> ```


#### ```FUNCTION zdb_suggest_terms(table_name regclass, fieldname text, base text, query text, max_terms bigint) RETURNS SET OF zdb_suggest_terms_response```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```fieldname```: The name of a field from which to derive term suggestions  
> ```base```:  a word from which suggestions will be created   
> ```query```: a full text query  
> ```max_terms```: maximum number of terms to return.  A value of zero means "all terms".
> 
> This function provides direct access to Elasticsearch's [term suggester](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters-term.html) and unlike zdb_significant_terms and zdb_tally, **can** be used with fields of type ```phrase```, ```phrase_array```, and ```fulltext```.  The results are MVCC-safe.  Returned terms are forced to upper-case.
> 
> If a stem is not specified, no results will be returned.  To match all terms: ```^.*```
> 
> Also, the ```base``` term is always returned as the first result.  If it doesn't exist in the index, it will have a count of zero.
> 
> Example:
> 
> ```
>SELECT * FROM zdb_suggest_terms('products', 'long_description', 'lang', '', 5000);
> term | count 
>------+-------
> LANG |     0
> LAND |     1
> LONG |     1
> ```

#### ```FUNCTION zdb_range_agg(table_name regclass, fieldname text, range_spec json, query text)```

> `table_name`:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> `fieldname`:  The name of a field from which to derive range aggregate buckets  
> `range_spec`:  JSON-formatted array that is compatible with Elasticsearch's [range aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html) range definition  
> `query`:  a full text query  
> 
> This function provides direct access to Elasticsearch's [range aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html) and can only be used with "numeric"-type fields.
> 
> The `range_spec` argument must be a properly-formed JSON array structure, for example:
> 
> ```
> [ 
>     { "key":"first bucket", "from":0, "to":100 },
>     { "from":100, "to":200 },
>     { "from":200 }
> ]
> ```
> 
> Returns a set based on the type: `zdb_range_agg_response AS (key text, low double precision, high double precision, doc_count int8)`.  NOTE:  the `low` and `high` columns could be null, depending on if you defined open-ended "from" or "to" ranges.
> 
> Example:
> 
> ```
> SELECT * FROM zdb_range_agg('products', 'price', '[ {"key":"cheap", "from":0, "to":100 }, { "from":100, "to":2000 }, {"key":"expensive", "from":1000 } ]', '');
>     key      | low  | high | doc_count 
>--------------+------+------+-----------
> cheap        |    0 |  100 |         0
> 100.0-2000.0 |  100 | 2000 |         2
> expensive    | 1000 |      |         4
```

#### ```FUNCTION zdb_tally(table_name regclass, fieldname text, stem text, query text, max_terms bigint, sort_order zdb_tally_order) RETURNS SET OF zdb_tally_response```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```fieldname```: The name of a field from which to derive terms  
> ```stem```:  a Regular expression by which to filter returned terms   
> ```query```: a full text query  
> ```max_terms```: maximum number of terms to return.  A value of zero means "all terms".
> ```sort_order```: how to sort the terms.  one of ```'count'```, ```'term'```, ```'reverse_count'```, ```'reverse_term'```
> 
> This function provides direct access to Elasticsearch's [terms aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html) and cannot be used with fields of type `fulltext`.  The results are MVCC-safe.  Returned terms are forced to upper-case.
> 
> If a stem is not specified, no results will be returned.  To match all terms: ```^.*```
> 
> Example:
> 
> ```
> SELECT * FROM zdb_tally('products', 'keywords', '^.*', 'base* or distance', 5000, 'term');
> 
>     term      | count 
>---------------+-------
> BASEBALL      |     1
> COMMUNICATION |     1
> PRIMITIVE     |     1
> SPORTS        |     1
> THOMAS EDISON |     1
> ```

## Views

#### ```VIEW zdb_index_stats```

A view that returns information about every "zombodb" index in the current database.  Note that this view can be slow because it includes a "SELECT count(*)" for each table with a "zombodb" index.

Example:

```
 SELECT * FROM zdb_index_stats;
 
                index_name                 |          url           | table_name | es_docs | es_size | es_size_bytes | pg_docs | pg_size | pg_size_bytes | shards | replicas 
-------------------------------------------+------------------------+------------+---------+---------+---------------+---------+---------+---------------+--------+----------
 tutorial.public.products.idx_zdb_products | http://localhost:9200/ | products   | 8       | 21 kB   |         21162 |       4 | 32 kB   |         32768 | 5      | 0
```

#### ```VIEW zdb_index_stats_fast```

Same as above, except the "SELECT count(*)" is substituted with ```pg_class.reltuples```, so the ```pg_docs``` coulmn becomes an estimate.

Example:

```
 SELECT * FROM zdb_index_stats_fast;
 
                index_name                 |          url           | table_name | es_docs | es_size | es_size_bytes | pg_docs_estimate | pg_size | pg_size_bytes | shards | replicas 
-------------------------------------------+------------------------+------------+---------+---------+---------------+------------------+---------+---------------+--------+----------
 tutorial.public.products.idx_zdb_products | http://localhost:9200/ | products   | 8       | 21 kB   |         21162 |                4 | 32 kB   |         32768 | 5      | 0
```
