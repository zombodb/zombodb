# ZomboDB SQL API

While the heart of ZomboDB is an index, the extension provides a number of SQL-callable functions for asking out-of-band questions of the underlying Elasticsearch index.


## Custom DOMAIN types

These custom domains are to be used in user tables as data types when you require text parsing and analysis for a given field.  The [type mapping](TYPE-MAPPING.md) documentation provides much more detail.

#### ```DOMAIN phrase AS text```

>This domain indicates to ZomboDB that the corresponding field should be analyzed in Elasticsearch.

#### ```DOMAIN fulltext AS text```

>Currently has exact same meaning as the ```phrase``` domain.  This might change in the future.
>

#### `DOMAIN fulltext_with_shingles AS text`

>Same as `fulltext` above, but provides higher performance when using right-truncated wildcards, especially when they're in a "quoted phrase", such as `body:"fried chick* is delicious"`.

#### ```DOMAIN phrase_array AS text[]```

>Similar to ```phrase``` but each array element will be analyzed.  It's spelled this way because Postgres does not currently support arrays over DOMAIN objects, ie ```phrase[]```.

#### Foreign Language Domains

A domain defined as `DOMAIN foo AS text` exists for the following languages: 

```
arabic, armenian, basque, brazilian, 
bulgarian, catalan, chinese, cjk, 
czech, danish, dutch, english, 
finnish, french, galician, german, 
greek, hindi, hungarian, indonesian, 
irish, italian, latvian, norwegian, 
persian, portuguese, romanian, russian, 
sorani, spanish, swedish, turkish, 
thai
```

## Custom Operators

#### ```OPERATOR ==> (LEFTARG=tid, RIGHTARG=text)```

>This operator is ZomboDB's "full text query" operator.
>Example:
>
>```SELECT * FROM table WHERE zdb('table', table.ctid) ==> 'full text query';```


## SQL Functions

#### ```FUNCTION zdb(table_name regclass, ctid tid) RETURNS tid```

> `table_name`: The name of a table with a ZomboDB index  
> `ctid`: A Postgres "tid" tuple pointer
>
>This function is required when creating "zombodb" indexes as the **first** column and when performing full text queries.
>
>returns the value of the second argument.
>
>Examples:
>
>`CREATE INDEX idxfoo ON table USING zombodb (zdb('table', table.ctid), zdb(table)) WITH (...);`
>`SELECT * FROM table WHERE zdb('table', ctid) ==> '...';`

#### ```FUNCTION zdb(r record) RETURNS json```

> ```r```: a record reference
> 
>This function is required as the **second** column when creating "zombodb" indexes via `CREATE INDEX`.
>
>returns a JSON-ified version of input record.
>
>Examples:
>
>```CREATE INDEX idxfoo ON table USING zombodb (zdb('table', table.ctid), zdb(table)) WITH (...);```

#### ```FUNCTION zdb_actual_index_record_count(table_name regclass, type text) RETURNS bigint```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```type```: the Elasticsearch type to inspect, one of 'xact' or 'data'
> 
> returns the actual number of documents contained in Elasticsearch for the index on the specified table name.
> 
> Example:
> 
> ```SELECT * FROM zdb_actual_index_record_count('table', 'data');```

#### `FUNCTION zdb_analyze_text(index_name regclass, analyzer_name text, data text) RETURNS SETOF zdb_analyze_text_response`

> `index_name`: The name of a ZomboDB index  
> `analyzer_name`: The name of an analyzer defined in the index, or any of the default Elasticsearch [language analyzers](https://www.elastic.co/guide/en/elasticsearch/reference/1.7/analysis-lang-analyzer.html)  
> `data`: The text to analyze
> 
> This function is useful for determing how a specific Elasticsearch analyzer is going to tokenize text.
> 
> returns a result set in the form of 
> 
> ```
> TYPE zdb_analyze_text_response AS (
>     token text, 
>     start_offset integer, 
>     end_offset integer, 
>     type text, 
>     position integer
> );
> ```
> 
> Example:
> 
> ```
> SELECT * FROM zdb_analyze_text('idxso_posts', 
>    'english', 
>    'the quick brown fox jumped over the lazy dog''s back');
> 
 token | start_offset | end_offset |    type    | position 
-------+--------------+------------+------------+----------
 quick |            4 |          9 | <ALPHANUM> |        2
 brown |           10 |         15 | <ALPHANUM> |        3
 fox   |           16 |         19 | <ALPHANUM> |        4
 jump  |           20 |         26 | <ALPHANUM> |        5
 over  |           27 |         31 | <ALPHANUM> |        6
 lazi  |           36 |         40 | <ALPHANUM> |        8
 dog   |           41 |         46 | <ALPHANUM> |        9
 back  |           47 |         51 | <ALPHANUM> |       10
(8 rows)
```

### `FUNCTION zdb_json_agg(table_name regclass, aggregration_json_spec json, query text) RETURNS json`

> `table_name`: The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index
> `aggregation_json_spec`: a correctly-defined Elasticsearch aggregation, in JSON
> `query`: a full text query
> 
> returns the Elasticsearch-created JSON results for the specified aggregation.  The data returned is MVCC-safe.
> 
> This function can be used when you need more advanced Elasticsearch aggregation support than ZomboDB provides out-of-the-box.
> 
> An example using Elasticsearch's "top hits" aggregation, against the tutorial `products` table:
> 
> ```sql
> tutorial=# SELECT *
tutorial-# FROM zdb_json_aggregate('products', '{
         "top-tags": {
             "terms": {
                 "field": "keywords",
                 "size": 3
             },
             "aggs": {
                 "top_tag_hits": {
                     "top_hits": {
                         "sort": [
                             {
                                 "availability_date": {
                                     "order": "desc"
                                 }
                             }
                         ],
                         "size" : 1
                     }
                 }
             }
         }
     }', 'wooden');
-[ RECORD 1 ]------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
zdb_json_aggregate | {"top-tags":{"doc_count_error_upper_bound":0,"sum_other_doc_count":4,"buckets":[{"key":"baseball","doc_count":1,"top_tag_hits":{"hits":{"total":1,"max_score":null,"hits":[{"_index":"tutorial.public.products.idx_zdb_products","_type":"data","_id":"0-2","_score":null,"sort":["2015-08-21"]}]}}},{"key":"box","doc_count":1,"top_tag_hits":{"hits":{"total":1,"max_score":null,"hits":[{"_index":"tutorial.public.products.idx_zdb_products","_type":"data","_id":"0-4","_score":null,"sort":["2015-07-01"]}]}}},{"key":"negative space","doc_count":1,"top_tag_hits":{"hits":{"total":1,"max_score":null,"hits":[{"_index":"tutorial.public.products.idx_zdb_products","_type":"data","_id":"0-4","_score":null,"sort":["2015-07-01"]}]}}}]}}
```

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
> #tally(fieldname, stem, max_terms, term_order [, shard_size] [, another aggregate])
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
> #json_agg({ ... Elasticsearch JSON aggregation ... })
>```
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
>The response is a JSON blob because it's quite difficult to project an arbitrary nested structure into a resultset with SQL.  The intent is that decoding of the response be application-specific.

#### `FUNCTION zdb_define_analyzer(name text, definition json) RETURNS void`
> `name`: The name to give the analyzer  
> `definition`: the JSON object definition of the analyzer
> 
> Allows you to define a custom Elasticsearch analyzer.  Once defined, you need to create a domain with the same name.
> 
> For example:
> 
> ```
> SELECT zdb_define_analyzer('my_analyzer_type', '{ "tokenizer": "standard" }');
> CREATE DOMAIN my_analyzer_type AS text;
> CREATE TABLE foo (id serial8 PRIMARY KEY,
>     field my_analyzer_type
> );
> ```
> 
> Only domains defined as `AS text` or `AS varchar(<size>)` are supported.
> 
> See the [type mapping](TYPE-MAPPING.md) documentation for for details.

#### `FUNCTION zdb_define_char_filter(name text, definition json) RETURNS void`
> `name`: The name to give the character filter  
> `definition`: the JSON object definition of the character filter
> 
> Allows you to define a custom Elasticsearch character filter to be used by a custom analyzer.
> 
> For example:
> 
> ```
> SELECT zdb_define_char_filter('zero_width_spaces', '{
            "type":       "mapping",
            "mappings": [ "\\u200C=> "] 
        }');
> ```
> 
> See the [type mapping](TYPE-MAPPING.md) documentation for for details.

#### `FUNCTION zdb_define_filter(name text, definition json) RETURNS void`
> `name`: The name to give the token filter  
> `definition`: the JSON object definition of the token filter
> 
> Allows you to define a custom Elasticsearch token filter to be used by a custom analyzer.
> 
> For example:
> 
> ```
> SELECT zdb_define_filter('english_stop', '{
          "type":       "stop",
          "stopwords":  "_english_" 
        }');
> ```
> 
> See the [type mapping](TYPE-MAPPING.md) documentation for for details.

#### `FUNCTION zdb_define_tokenizer(name text, definition json) RETURNS void`
> `name`: The name to give the tokenizer  
> `definition`: the JSON object definition of the tokenizer
> 
> Allows you to define a custom Elasticsearch tokenizer to be used by a custom analyzer.
> 
> For example:
> 
> ```
> SELECT zdb_define_tokenizer('my_ngram_tokenizer', '{
                        "type" : "nGram",
                        "min_gram" : "2",
                        "max_gram" : "3",
                        "token_chars": [ "letter", "digit" ]
                    }');
> ```
> 
> See the [type mapping](TYPE-MAPPING.md) documentation for for details.


#### `FUNCTION zdb_define_mapping(table_name regclass, field_name name, definition json) RETURNS void`
> `table_name`: The table name to receive this custom field mapping
> `field_name`: The file in the table to which this custom field mapping will be assigned
> 
> Allows you to define a custom Elasticsearch field mapping for a field.
> 
> For example:
> 
> ```
> SELECT zdb_define_mapping('my_table', 'the_field', '{
>    "type": "string",
>    "store": true,
>    "include_in_all": false
> }');
> 
> See the [type mapping](TYPE-MAPPING.md) documentation for for details.

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

#### `FUNCTION zdb_dump_query(table_name regclass, query text) RETURNS text`

> `table_name`:  The name of a table with a ZomboDB index  
> `query`: a full text query
> 
> returns the Elasticsearch QueryDSL that would be executed, fully resolved with index links
> 
> Example:
> 
```
SELECT * FROM zdb_dump_query('test', 'subject:(this is a test)');
                   zdb_dump_query                   
----------------------------------------------------
 {                                                 +
   "bool" : {                                      +
     "must" : {                                    +
       "terms" : {                                 +
         "subject" : [ "this", "is", "a", "test" ],+
         "minimum_should_match" : "4"              +
       }                                           +
     }                                             +
   }                                               +
 }
(1 row)
```


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

#### ```FUNCTION zdb_extended_stats(table_name regclass, fieldname text [, is_nested boolean], query text) RETURNS SET OF zdb_extended_stats_response```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```fieldname```: A numeric field in the specified table  
> ```is_nested```: Optional argument to indicate that the terms should only come from matching nested object sub-elements.  Default is `false`    
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

#### `FUNCTION zdb_get_index_field_lists(table_name regclass) RETURNS SETOF zdb_get_index_field_lists_response`

> `table_name`:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index
> 
> returns a resultset describing all the field lists that are defined for `table_name`.
> 
> Example:
> 
> ```
> SELECT * FROM zdb_get_index_field_lists('some_table');
    fieldname     |           fields           
------------------+----------------------------
 title_and_author | {title,author}
 hashes           | {sha1,md5}
(2 rows)

> ```
 
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
> {"mappings":{"xact":{"_meta":{"primary_key":"id"},"date_detection":false,"_all":{"enabled":false},"_field_names":{"enabled":false,"type":"_field_names","index":"no"},"properties":{"_cmax":{"type":"integer","fielddata":{"format":"disabled"}},"_cmin":{"type":"integer","fielddata":{"format":"disabled"}},"_partial":{"type":"boolean","fielddata":{"format":"disabled"}},"_xmax":{"type":"integer","fielddata":{"format":"disabled"}},"_xmax_is_committed":{"type":"boolean","fielddata":{"format":"disabled"}},"_xmin":{"type":"integer","fielddata":{"format":"disabled"}},"_xmin_is_committed":{"type":"boolean","fielddata":{"format":"disabled"}}}},"data":{"_meta":{"primary_key":"id"},"date_detection":false,"_all":{"enabled":true,"analyzer":"phrase"},"_parent":{"type":"xact"},"_routing":{"required":true},"_field_names":{"enabled":false,"type":"_field_names","index":"no"},"_source":{"enabled":false},"properties":{"discontinued":{"type":"boolean"},"id":{"type":"long","store":true,"include_in_all":false},"inventory_count":{"type":"integer","store":true,"include_in_all":false},"keywords":{"type":"string","norms":{"enabled":false},"index_options":"docs","analyzer":"exact"},"long_description":{"type":"string","norms":{"enabled":false},"analyzer":"fulltext","fielddata":{"format":"disabled"},"include_in_all":false},"name":{"type":"string","norms":{"enabled":false},"index_options":"docs","analyzer":"exact"},"price":{"type":"long","store":true,"include_in_all":false},"short_summary":{"type":"string","norms":{"enabled":false},"analyzer":"phrase","fielddata":{"format":"disabled"}}}}}}
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

#### `FUNCTION zdb_multi_search(table_names regclass[], user_identifiers text[], field_names[][], query text) RETURNS SETOF zdb_multi_search_response`

> `table_names`:  An array of tables (or views) with ZomboDB indexes to search at the same time  
> `user_identifiers`:  An array of arbitrary identifiers for each table.  This could be useful for a client application to distinguish between result rows that use the same underlying table.  
> `field_names`:  A 2-d array of field names to return for each table.  If it is null, all fields (except those of type `fulltext`) will be returned.
> `query`: a full text query
> 
> This function searches the array of tables using the specified full text query, and returns the top 10 documents from each in descending score order.
> 
> The scores and document field data are aggregated, such that at most, the returned resultset will only have as many rows as `table_names` has elements.  If no results were found for a particular table, it is excluded from the results.
> 
> The result is of the type:
> 
> ```
> TYPE zdb_multi_search_response AS (
>    table_name regclass, 
>    user_identifier text,
>    query text, 
>    total int8, 
>    score float4[], 
>    row_data json
> );
> ```
> 
> The `table_name` column indicates the table name of the matching row  
> The `user_identifier` column indicates the provided value for the current table
> The `query` column indicates which query the row matched  
> The `total` column indicates the total number of matching documents  
> The `score` column indicates the Elasticsearch-calculated scores for each matching row  
> The `row_data` column is a json array of the `row_to_json()` for the top 10 matching documents.  If the `field_names` argument is non-null, it will contain only the fields you specified.  Otherwise, all fields, except those of type `fulltext` are returned  
> 
> Note that if one of the `table_names` elements is actually a view, the view must contain the primary key field name from the underlying table (as determined by `zdb_determine_index()`), otherwise an ERROR will be thrown.
> 
> Example (using the "tutorial" database that comes with ZomboDB sources):
> 
> ```
> select * from zdb_multi_search(ARRAY['so_posts', 'so_users'], ARRAY['a', 'b'], NULL, 'java javascript');
 table_name | user_identifier |      query      | total |                                       score                                       |                                                                                               
------------+-----------------+-----------------+-------+-----------------------------------------------------------------------------------+-----------------------------------------------------------------------------------------------
 so_posts   | a               | java javascript |  1171 | {11.0463,10.4817,10.3789,9.45525,9.36948,9.31416,8.66982,8.64361,8.26008,8.25053} | [{"accepted_answer_id":70607,"answer_count":2,"closed_date":"2012-01-30 22:51:06.303-05","comm...
 so_users   | b               | java javascript |  1309 | {3.32197,3.23258,3.20018,3.14003,2.99723,2.96903,2.9636,2.90212,2.87772,2.75175}  | [{"account_id":29943,"age":30,"creation_date":"2012-02-22 18:54:08.103-05","display_name":"Nac...
(2 rows)
> ```

#### `FUNCTION zdb_multi_search(table_names regclass[], user_identifiers text[], queries text[]) RETURNS SETOF zdb_multi_search_response`
>
> This function is similar to the above function that only takes a single query, except this function requires that the `table_names` and `queries` arguments be of the same length.  Each table in the `table_names` array is searched using the corresponding query from the `queries` array.
> 
> This allows you to search multiple tables at the same time, but each table uses a different query.



#### ```FUNCTION zdb_significant_terms(table_name regclass, fieldname text [, is_nested boolean], stem text, query text, max_terms bigint) RETURNS SET OF zdb_significant_terms_response```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```fieldname```: The name of a field from which to derive significant terms
> ```is_nested```: Optional argument to indicate that the terms should only come from matching nested object sub-elements.  Default is `false`    
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

#### ```FUNCTION zdb_range_agg(table_name regclass, fieldname text [, is_nested boolean], range_spec json, query text)```

> `table_name`:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> `fieldname`:  The name of a field from which to derive range aggregate buckets. If this is a date field, a date range aggregate will be executed.  
> ```is_nested```: Optional argument to indicate that the terms should only come from matching nested object sub-elements.  Default is `false`    
> `range_spec`:  JSON-formatted array that is compatible with Elasticsearch's [range aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html) range definition if the field is not a date. If the field is a date, utilize Elasticsearch's [date range aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html) syntax. 
> `query`:  a full text query  
> 
> This function provides direct access to Elasticsearch's [range aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html) for "numeric"-type fields.
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
>
> This function also provides direct access to Elasticsearch's [date range aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html) for date fields.

> In this case, the `range_spec` argument must be a properly-formed JSON array structure for the type of date field being queried, for example for the product.availability_date field:
> ```
> [ 
>     { "key": "early", "to": "2015-08-01"},
>     { "from":"2015-08-01", "to":"2015-08-15" }, 
>     { "from":"2015-08-15" } 
> ]
> ```
>
> Which could yield:
>
> ```
> SELECT * FROM zdb_range_agg('products', 'availability_date', '[ { "key": "early", "to": "2015-08-01"},{"from":"2015-08-01", "to":"2015-08-15"}, {"from":"2015-08-15"} ]', '');
>                        key                        |      low      |     high      | doc_count 
>---------------------------------------------------+---------------+---------------+-----------
> early                                             |               | 1438387200000 |         1
> 2015-08-01T00:00:00.000Z-2015-08-15T00:00:00.000Z | 1438387200000 | 1439596800000 |         1
> 2015-08-15T00:00:00.000Z-*                        | 1439596800000 |               |         2
> ```
>
> If the field is of type `timestamp without time zone`, this structure would be appropriate:
>
> ```
> [ 
>     { "from":"2015-01-01 00:00:00", "to":"2015-01-01 15:30:00" }, 
>     { "from":"2015-01-02 01:00:00" } 
> ]
>```

### ```FUNCTION zdb_score(table_name regclass, ctid tid) RETURNS float4```
> `table_name`:  The name of the table or view that is being queried and from which you want scores  
> `ctid`: the system column named `ctid` from the underlying table being queried
> 
> The `zdb_score()` function retrieves the relevancy score value for each document, as determined by Elasticsearch.  It is designed to be used in either (or both) the query target list or its `ORDER BY` clause.
> 
> Example:
> 
> ```
> SELECT zdb_score('products', products.ctid), * 
>   FROM products 
>  WHERE zdb('products', products.ctid) ==> 'sports or box' 
>  ORDER BY zdb_score('products', products.ctid) desc;
> 
 zdb_score | id |   name   |               keywords               |         short_summary          |                                
-----------+----+----------+--------------------------------------+--------------------------------+--------------------------------
 0.0349381 |  4 | Box      | {wooden,box,"negative space",square} | Just an empty box made of wood | A wooden container that will ev
 0.0252144 |  2 | Baseball | {baseball,sports,round}              | It's a baseball                | Throw it at a person with a big
(2 rows)
```


#### ```FUNCTION zdb_tally(table_name regclass, fieldname text [, is_nested boolean], stem text, query text, max_terms bigint, sort_order zdb_tally_order [, shard_size int DEFAULT 0]) RETURNS SET OF zdb_tally_response```

> ```table_name```:  The name of a table with a ZomboDB index, or the name of a view on top of a table with a ZomboDB index  
> ```fieldname```: The name of a field from which to derive terms  
> ```is_nested```: Optional argument to indicate that the terms should only come from matching nested object sub-elements.  Default is `false`    
> ```stem```:  a Regular expression by which to filter returned terms, or a date interval if the specified `fieldname` is a date or timestamp    
> ```query```: a full text query  
> ```max_terms```: maximum number of terms to return.  A value of zero means "all terms".
> ```sort_order```: how to sort the terms.  one of ```'count'```, ```'term'```, ```'reverse_count'```, ```'reverse_term'```  
> `shard_size`: optional parameter that tells Elasticsearch how many terms to return from each shard.  Default is zero, which means all terms  
> 
> This function provides direct access to Elasticsearch's [terms aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html) and cannot be used with fields of type `fulltext`.  The results are MVCC-safe.  Returned terms are forced to upper-case.
> 
> If a stem is not specified, no results will be returned.  
>
> To match all terms: ```^.*```
> 
> If the specifield `fieldname` is a date/timestamp, then one of the following values are allowed for aggregating values into histogram buckets of the specified interval: `year, quarter, month, week, day, hour, minute, second`.  In all cases, an optional offset value can be specified.  For example:  `week:-1d` will offset the dates by one day so that the first day of the week will be considered to be Sunday (instead of the default of Monday).
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
> 
> Regarding the `is_nested` argument, consider data like this:
> 
> ```
> row #1: contributor_data=[ 
>   { "name": "John Doe", "age": 42, "location": "TX", "tags": ["active"] },
>   { "name": "Jane Doe", "age": 36, "location": "TX", "tags": ["nice"] }
]
>
>row #2: contributor_data=[ 
>   { "name": "Bob Dole", "age": 92, "location": "KS", "tags": ["nice", "politician"] },
>   { "name": "Elizabth Dole", "age": 79, "location": "KS", "tags": ["nice"] }
>]
> ```
> 
> And a query where `is_nested` is false:
> 
> ```
> SELECT * FROM zdb_tally('products', 'contributor_data.name', false, '^.*', 'contributor_data.location:TX AND contributor_data.tags:nice', 5000, 'term');
> ```
> 
> returns:
> 
>```
>    term   | count 
>----------+-------
>  JANE DOE |     1
>  JOHN DOE |     1
>(2 rows)
>```
>
>Whereas, if `is_nested` is true, only "JANE DOE" is returned because it's the only subelement of `contributor_data` that matched the query:
> 
> ```
> SELECT * FROM zdb_tally('products', 'contributor_data.name', true, '^.*', 'contributor_data.location:TX WITH contributor_data.tags:nice', 5000, 'term');
> ```
> 
> returns:
> 
> ```
>    term   | count 
>----------+-------
>  JANE DOE |     1
>(1 row)
>```

#### ```FUNCTION zdb_termlist(table_name regclass, fieldname text, prefix text, startAt text, size int4) RETURNS SET OF zdb_termlist_response```
>
> `table_name`: The name of a table from which to select terms  
> `fieldname`: The name of the field from which to select terms  
> `prefix`: The prefix substring each returned term should start with, can be the empty string (ie, start at top)
> `startAt`: The exact term value from which termlist selection should begin.  Can be NULL, otherwise the term **must** share the `prefix`
> `size`: The total number of terms to return
> 
> This function directly examines the underlying Lucene index for a table/field and returns `size` terms matching the specified `prefix`.
> 
> Set the `startAt` argument to a non-NULL term (sharing the prefix) to enable paging through large lists of terms.
> 
> Returns a resultset in the form of
> 
> ```
> CREATE TYPE zdb_termlist_response AS (
  term text,
  totalfreq bigint,
  docfreq bigint
);
> ```
> 
> Where `totalfreq` is the total number of times the `term` appears in that field, and `docfreq` is the total number of matching documents.
> 
> Terms are returned in ABC order, as they're ordered in the underlying Lucene index, which means that Elasticsearch is doing the sorting, not Postgres.  Depending on locale settings between your Elasticsearch cluster and Postgres, an `ORDER BY term` clause could order the terms differently, and should generally be avoided.
> 
> >NOTE:
> >
> >It's worth noting that the results from this function are completely unfiltered and pay no attention to Postgres' transaction visibility rules.
> >
> > Knowing that ZomboDB stores what Postgres might consider "dead rows" in the Elasticsearch index (until a `VACUUM` occurs), terms from these "dead rows" will also be returned.  
> > 
> > A "dead row" is one that has been DELETEd in Postgres, or the old version of a row that has been UPDATEd.  This also means that the `totalfreq` and `docfreq` values may be larger than what is actually searchable via a normal `SELECT` statement.
> 
> 
> Example:
> 
> ```
> select * from zdb_termlist('products', 'long_description', 't', NULL, 20);
     term      | totalfreq | docfreq 
---------------+-----------+---------
 telemarketers |         1 |       1
 that          |         1 |       1
 the           |         1 |       1
 they          |         1 |       1
 things        |         1 |       1
 this          |         1 |       1
 throw         |         1 |       1
 to            |         1 |       1
(8 rows)

> ```

#### `FUNCTION zdb_update_mapping(table_name regclass) RETURNS void`
> `table_name`:  The name of a table with a ZomboDB index
> 
> This function updates the backing Elasticsearch index's mapping and settings.  Useful to call after you `ALTER INDEX` to change the number of replicas or the refresh_interval.  Also can be called when certain field data type changes have been made or when new columns (with no default value) have been added.
> 
> Example:
> 
> ```sql
> SELECT * FROM zdb_update_mapping('pdocuts');
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
