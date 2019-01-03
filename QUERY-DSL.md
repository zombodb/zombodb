# Query DSL

ZomboDB supports a few different ways to generate Elasticsearch-compatible queries.  You can use Elasticsearch's query string syntax, directly generate its QueryDSL in JSON form, or use ZomboDB's SQL builder API which closely mirrors Elasticsearch's QueryDSL.

Wherever ZomboDB wants to you specify a query, which is typically `SELECT` statements and aggregate functions, you can interchangeably use any of the below query forms.

To use a `SELECT` statement as an example, lets suppose we want to select all the rows that contain the terms "cats and dogs" regardless of field.  The basic query template looks like:

```sql
SELECT * FROM table WHERE table ==> <cats and dogs query here>
```

Note that regardless of the way you query, know that essentially you're generating [Elasticsearch QueryDSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) in JSON.

ZomboDB tries to abstract this fact away by using a custom Postgres type called `zdbquery` that can be cast to/from `text`, `json`, and `jsonb`.  As such, the right-hand-side of ZomboDB's `==>` operator is of type `zdbquery`.

The goal of this document is not to teach the ins-and-outs of Elasticsearch's query capabilities.  It is recommented you reference its documentation when the information here is not sufficient.  Where approrpiate, links to specific Elasticsearch Query DSL clauses are provided below.

That said, lets discuss how to write our example query using ZomboDB's supported query forms.


### Query String Syntax

The Query String Syntax is a plain-text query language [implemented by Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html).  It's "google"-like in that you can simply specify free-form, unqualified words and "quoted phrases" and Elasticsearch figures out the matching documents.  Additionally, it supports a fairly sophsicated boolean syntax that includes field qualification, proximity, ranges, wildcards, etc.

Using the query string syntax, searching for "cats and dogs" could be any of the following:

```sql
SELECT * FROM table WHERE table ==> '+cats +dogs';
SELECT * FROM table WHERE table ==> 'cats AND dogs';
```

To show what's happening behind the scenes, ZomboDB is actually generating Elasticsearch QueryDSL JSON for the above queries:

```sql
SELECT '+cats +dogs'::zdbquery::json;
                   json                   
------------------------------------------
 {"query_string":{"query":"+cats +dogs"}}

SELECT 'cats AND dogs'::zdbquery::json;
                    json                    
--------------------------------------------
 {"query_string":{"query":"cats AND dogs"}}

```

So you can see that ZomboDB is directly using Elasticsearch's Query String query.

### Direct JSON

While the above Query String Syntax is easy for humans to read and type, it doesn't expose every feature of Elasticsearch's QueryDSL.  Enter direct json.

```sql
SELECT * FROM table WHERE table ==> '{"bool":{"must":[{"term":{"zdb_all":{"value":"cats"}}},{"term":{"zdb_all":{"value":"dogs"}}}]}}';
```

You have the full gamut of the Elasticsearch QueryDSL available to you with this form.  This is likely best used when you're programatically generating queries.

> Note that the field `zdb_all` is ZomboDB's version of Elasticsearch's "_all" field, except `zdb_all` is enabled for all versions of Elasticsearch.  It is also configured as the default search field for every ZomboDB index, which is why it wasn't specified in the Query String Syntax examples, but is here.

### SQL Builder API

ZomboDB also exposes nearly all of Elasticsearch's QueryDSL queries as SQL functions, located in a schema named `dsl`.  These functions all return a `zdbquery`, and can be composed together to build complex queries.  The primary advantages of this API are that these functions are syntax- and type-checked by Postgres, so you'll catch malformed queries sooner.

In general, each function models its corresponding Elasticsearch query exactly.  Default values are used for arguments in all places where Elasticsearch provides defaults for properties, and arguments are required where Elasticsearch requires the corresponding property.  Postgres VARIADIC function arguments are used in most cases where Elasticsearch expects an array of queries or values.

They're designed to be used with defaults in the common cases, and then otherwise should be used using Postgres' "named arguments" function call syntax to improve readability.

All of the functions are briefly described below, but here's some examples for our "cats and dogs" queries, plus a few more examples.

```sql
SELECT * FROM table WHERE table ==> dsl.and('cats', 'dogs');
SELECT * FROM table WHERE table ==> dsl.and(dsl.term('zdb_all', 'cats'), dsl.term('zdb_all', 'dogs'));
```

Behind the scenes, ZomboDB is just generating the QueryDSL JSON for you:

```sql
SELECT dsl.and('cats', 'dogs')::json;
                                          must                                           
-----------------------------------------------------------------------------------------
 {"bool":{"must":[{"query_string":{"query":"cats"}},{"query_string":{"query":"dogs"}}]}}
 
SELECT dsl.and(dsl.term('zdb_all', 'cats'), dsl.term('zdb_all', 'dogs'))::json;
                                              must                                               
-------------------------------------------------------------------------------------------------
 {"bool":{"must":[{"term":{"zdb_all":{"value":"cats"}}},{"term":{"zdb_all":{"value":"dogs"}}}]}}
```

Lets say you want to find all rows that contain cats with an age greater than 3 years.  This example shows, with the `range()` function, using Postgres "named arugments" function call syntax so that you can specifiy only the bounds of the range you need.  We're also mix-and-matching between the plain text Query String Syntax (`'cats'`) and the builder API (`must()` and `range()`):

```sql
SELECT * FROM table WHERE table ==> dsl.and('cats', dsl.range(field=>'age', gt=>3));
```

Which rewrites to:

```sql
SELECT dsl.and('cats', dsl.range(field=>'age', gt=>3))::json;
                                        must                                        
------------------------------------------------------------------------------------
 {"bool":{"must":[{"query_string":{"query":"cats"}},{"range":{"age":{"gt":"3"}}}]}}
```

One of the more powerful benefits of the Builder API is that it allows you to generate Postgres prepared statements for your text-search queries.  For example:

```sql
PREPARE example AS SELECT * FROM table WHERE table ==> dsl.and($1, dsl.range(field=>'age', gt=>$2));
```

Now we can execute that query using a different search term and age range:

```sql
EXECUTE exampe('cats', 3);
EXECUTE exampe('dogs', 7);
EXECUTE exampe('elephants', 23);
```

Using prepared statements is extremely important to avoid SQL-injection attacks.  ZomboDB makes this possible for your Elasticsearch QueryDSL query clauses too.  Any argument to any of the functions can become a prepared statement arugment that you can change at EXECUTE time.


### Sorting and Limiting Results

ZomboDB allows you to limit the number of rows returned, and their sort order, similar to the SQL `LIMIT` and `ORDER BY` clauses, except they're specified as part of the Elasticsearch query, and the sorting/limiting happens within Elasticsearch.  In general, this is significantly faster than having Postgres do it.

The following functions are designed to wrap the query you want to execute, on the outer levels.  For example, to return only 10 rows:

```sql
SELECT * FROM table WHERE table ==> dsl.limit(10, dsl.term('title', 'cat'));
```

Or to return 10 rows sorted by `id`:

```sql
SELECT * FROM table WHERE table ==> dsl.sort('id', 'asc', dsl.limit(10, dsl.term('title', 'cat')));
```

## Sort and Limit Functions

#### `dsl.limit()`

```sql
FUNCTION dsl.limit(
	limit bigint, 
	query zdbquery
) RETURNS zdbquery
```

Limits the number of rows returned to the specified `limit` limit.  If the query doesn't otherwise contain a `dsl.sort()` (see below), then the results returned are first sorted by `_score` in `desc`ending order.  This ensures that ZomboDB returns the top scoring documents. 

---

#### `dsl.offset()`

```sql
FUNCTION dsl.offset(
	offset bigint, 
	query zdbquery
) RETURNS zdbquery
```

Similar to the SQL `OFFSET` clause, allows you to start returning results from a point other than the start.

---

#### `dsl.sort()`

```sql
FUNCTION dsl.sort(
	sort_field text, 
	sort_direction dsl.es_sort_directions, -- one of 'asc' or 'desc'
	query zdbquery
) RETURNS zdbquery
```

Sort the results returned from Elasticsearch by an arbitrary field.

Because the SQL standard doesn't guarantee result ordering unless the query contains an `ORDER BY` clause, you should use `ORDER BY` in conjunction with this function.  For example:

```sql
SELECT * FROM table WHERE table ==> dsl.sort('id', 'asc', 'cats AND dogs') ORDER BY id asc;
```

In practice, using `dsl.sort()` only makes sense when combined with `dsl.limit()`.

There's an overloaded version of `dsl.sort()` (described below) that allows for more complex sorting descriptors.

---

#### `dsl.sd()`

```sql
FUNCTION dsl.sd(
	field text, 
	"order" dsl.es_sort_directions, 
	mode dsl.es_sort_modes DEFAULT NULL
) RETURNS dsl.es_sort_descriptor
```

`dsl.sd()` (which is short for "sort descriptor") allows you to contruct an object that represents sorting.  It is designed to be used as the arguments to `dsl.sort_many(zdbquery, VARIADIC dsl.es_sort_descriptor[])` (defined below).

The possible values for the `mode` argument are `min`, `max`, `sum`, `avg`,  and `median`.  These are documented here:  https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html#_sort_mode_option

Example:

```sql
SELECT * FROM table WHERE table ==> dsl.sort_many(dsl.match_all(), dsl.sd('field', 'asc'), dsl.sd('price', 'desc', 'avg'));
```

---

#### `dsl.sd_nested()`

```sql
FUNCTION dsl.sd_nested(
	field text, "order" 
	dsl.es_sort_directions, 
	nested_path text, 
	nested_filter zdbquery DEFAULT NULL, 
	mode dsl.es_sort_modes DEFAULT NULL
) RETURNS dsl.es_sort_descriptor
```

`dsl.sd_nested()` (which is short for "nested field sort descriptor") allows you to contruct an object that represents sorting for a nested field.  It is designed to be used as an argument to `dsl.sort_many(zdbquery, VARIADIC dsl.es_sort_descriptor[])` (defined below).

The possible values for the `mode` argument are `min`, `max`, `sum`, `avg`,  and `median`.  These are documented here:  https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-sort.html#_sort_mode_option

Example:

```sql
SELECT * FROM table WHERE table ==> dsl.sort_many(dsl.match_all(), dsl.sd_nested('offer.price', 'asc', 'offer', dsl.match_all(), 'avg'), dsl.sd('id', 'desc', 'avg'));
```

The above would first sort the results by the average value from the nested field `offer.price` in ascending order, and then by `id` in descending order.

---

#### `dsl.sort_many()`

```sql
FUNCTION dsl.sort_many(
	query zdbquery, 
	VARIADIC descriptors dsl.es_sort_descriptor[]
) RETURNS zdbquery
```

This is similar to the `dsl.sort()` function described above, however it requires the query be the first argument and allows for a variable list of sort descriptors, that should be generated using the `dsl.sd()` or `dsl.sd_nested()` functions.

Example:

```sql
SELECT * FROM table WHERE table ==> dsl.sort_many(dsl.match_all(), dsl.sd('field', 'asc'), dsl.sd('price', 'desc', 'avg'));
```

---

#### `dsl.sort_direct()`

```sql
FUNCTION dsl.sort_direct(
	sort_json json, 
	query zdbquery
) RETURNS zdbquery
```

This function allows you to specify direct json to describe how Elasticsearch should sort the results.

In the Elasticearch `_search` request body, this is the top-level `"sort"` property (https://www.elastic.co/guide/en/elasticsearch/reference/5.6/search-request-sort.html#nested-sorting).

Example:

```sql
SELECT * FROM table WHERE table ==> dsl.sort_direct('{
        "_script" : {
            "type" : "number",
            "script" : {
                "lang": "painless",
                "source": "doc[''field_name''].value * params.factor",
                "params" : {
                    "factor" : 1.1
                }
            },
            "order" : "asc"
        }
    }', dsl.match_all());
```

---

#### `dsl.min_score()`

```sql 
FUNCTION dsl.min_score(
	min_score real, 
	query zdbquery
) RETURNS zdbquery
```

This allows you to specify Elastisearch's [`min_score`](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-min-score.html) search property to ensure documents with a score less than the specified value are excluded from the results.


## SQL Builder API Functions

### Simple Boolean Functions

#### `dsl.and()`

```sql
FUNCTION dsl.and(
	VARIADIC queries zdbquery[]
) RETURNS zdbquery
```

Generates an Elasticsearch `bool` query where all the arguments are part of the `must` clause.

--- 

#### `dsl.or()`

```sql
FUNCTION dsl.or(
	VARIADIC queries zdbquery[]
) RETURNS zdbquery
```

Generates an Elasticsearch `bool` query where all the arguments are part of the `should` clause.

--- 

#### `dsl.not()`

```sql
FUNCTION dsl.not(
	VARIADIC queries zdbquery[]
) RETURNS zdbquery
```

Generates an Elasticsearch `bool` query where all the arguments are part of the `must_not` clause.


### Elasticsearch "bool" Query Support

#### `dsl.bool()`

```sql
FUNCTION dsl.bool(
	VARIADIC queries dsl.esqdsl_bool_part
) RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html

This function represents the Elasticsearch `bool` query.  It takes one or more "boolean part" queries, which are generated using the SQL functions named `dsl.must()`, `dsl.must_not()`, `dsl.should()`, and `dsl.filter()`.

Multiple usages of the same boolean part will be merged together automatically.

Example:

```sql
SELECT dsl.bool(
        dsl.must('beer', 'wine', 'cheese'),
        dsl.must_not('beer', 'wine', 'cheese'),
        dsl.should('beer', 'wine', 'cheese'),
        dsl.filter('beer', 'wine', 'cheese'),
        dsl.must('lettuce', 'tomato', 'bread') -- we used dsl.must() above too!
    );
```

Which ultimately generates the following Elasticsearch QueryDSL:

```json
{
  "bool": {
    "filter": [
      {
        "query_string": {
          "query": "beer"
        }
      },
      {
        "query_string": {
          "query": "wine"
        }
      },
      {
        "query_string": {
          "query": "cheese"
        }
      }
    ],
    "must": [
      {
        "query_string": {
          "query": "beer"
        }
      },
      {
        "query_string": {
          "query": "wine"
        }
      },
      {
        "query_string": {
          "query": "cheese"
        }
      },
      {
        "query_string": {
          "query": "lettuce"
        }
      },
      {
        "query_string": {
          "query": "tomato"
        }
      },
      {
        "query_string": {
          "query": "bread"
        }
      }
    ],
    "must_not": [
      {
        "query_string": {
          "query": "beer"
        }
      },
      {
        "query_string": {
          "query": "wine"
        }
      },
      {
        "query_string": {
          "query": "cheese"
        }
      }
    ],
    "should": [
      {
        "query_string": {
          "query": "beer"
        }
      },
      {
        "query_string": {
          "query": "wine"
        }
      },
      {
        "query_string": {
          "query": "cheese"
        }
      }
    ]
  }
}
```


---

#### `dsl.must()`

```sql
FUNCTION dsl.must (
	VARIADIC queries zdbquery[])
RETURNS dsl.esqdsl_must
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html

This is the `must` clause of the Elasticsearch QueryDSL `bool` query.  The queries must appear in matching documents and will contribute to the score.

This function is designed to be used with the `must` argument of `dsl.bool()`.  Its arguments can be or or more of any of ZomboDB's `dsl` functions that return a type of `zdbquery`.

---

#### `dsl.must_not()`

```sql
FUNCTION dsl.must_not (
	VARIADIC queries zdbquery[])
RETURNS dsl.esqdsl_must_not
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html

This is the `must_not` clause of the Elasticsearch QueryDSL `bool` query.  The queries must not appear in the matching documents. Clauses are executed in filter context meaning that scoring is ignored and clauses are considered for caching. Because scoring is ignored, a score of 0 for all documents is returned.

This function is designed to be used with the `must_not` argument of `dsl.bool()`.  Its arguments can be or or more of any of ZomboDB's `dsl` functions that return a type of `zdbquery`.

---

#### `dsl.should()`

```sql
FUNCTION dsl.should (
	VARIADIC queries zdbquery[])
RETURNS dsl.esqdsl_should
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html

This is the `should` clause of the Elasticsearch QueryDSL `bool` query.  The queries should appear in matching documents and will contribute to the score.

This function is designed to be used with the `should` argument of `dsl.bool()`.  Its arguments can be or or more of any of ZomboDB's `dsl` functions that return a type of `zdbquery`.

---

#### `dsl.filter()`

```sql
FUNCTION dsl.filter (
	VARIADIC queries zdbquery[])
RETURNS dsl.esqdsl_filter
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-bool-query.html

This is the `filter` object of an Elasticsearch QueryDSL `bool`.

The clause (query) must appear in matching documents. However unlike must the score of the query will be ignored. Filter clauses are executed in filter context, meaning that scoring is ignored and clauses are considered for caching.
 
This function is designed to be used with the `filter` argument of `dsl.bool()`.  Its arguments can be or or more of any of ZomboDB's `dsl` functions that return a type of `zdbquery`.

### Elasticsearch Query DSL Support
   
#### `dsl.boosting()`
    
```sql
FUNCTION dsl.boosting (
	positive zdbquery,
	negative zdbquery,
	negative_boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-boosting-query.html

The boosting query can be used to effectively demote results that match a given query. Unlike the "NOT" clause in bool query, this still selects documents that contain undesirable terms, but reduces their overall score.
 
---

#### `dsl.common()`

```sql
FUNCTION dsl.common (
	field text,
	query text,
	boost real DEFAULT NULL,
	cutoff_frequency real DEFAULT NULL,
	analyzer text DEFAULT NULL,
	minimum_should_match text DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-common-terms-query.html

The common terms query is a modern alternative to stopwords which improves the precision and recall of search results (by taking stopwords into account), without sacrificing performance.
 
---

#### `dsl.constant_score()`

```sql
FUNCTION dsl.constant_score (
	query zdbquery,
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-constant-score-query.html

A query that wraps another query and simply returns a constant score equal to the query boost for every document in the filter. Maps to Lucene ConstantScoreQuery.
 
---

#### `dsl.dis_max()`

```sql
FUNCTION dsl.dis_max (
	queries zdbquery[],
	boost real DEFAULT NULL,
	tie_breaker real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-dis-max-query.html

A query that generates the union of documents produced by its subqueries, and that scores each document with the maximum score for that document as produced by any subquery, plus a tie breaking increment for any additional matching subqueries.
 
---

#### `dsl.field_exists()`

```sql
FUNCTION dsl.field_exists (
	field text)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html


Returns documents that have at least one non-null value in the specified field
 
---

#### `dsl.field_missing()`

```sql
FUNCTION dsl.field_missing (
	field text)
RETURNS zdbquery
```

The inverse of `dsl.field_exists()`.  Returns documents that have no value in the specified field
 
---

#### `dsl.fuzzy()`

```sql
FUNCTION dsl.fuzzy (
	field text,
	value text,
	boost real DEFAULT NULL,
	fuzziness integer DEFAULT NULL,
	prefix_length integer DEFAULT NULL,
	max_expansions integer DEFAULT NULL,
	transpositions boolean DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html

The fuzzy query uses similarity based on Levenshtein edit distance.
 
---

#### `dsl.match()`

```sql
FUNCTION dsl.match (
	field text,
	query text,
	boost real DEFAULT NULL,
	analyzer text DEFAULT NULL,
	minimum_should_match text DEFAULT NULL,
	lenient boolean DEFAULT NULL,
	fuzziness integer DEFAULT NULL,
	fuzzy_rewrite text DEFAULT NULL,
	fuzzy_transpositions boolean DEFAULT NULL,
	prefix_length integer DEFAULT NULL,
	zero_terms_query dsl.es_match_zero_terms_query DEFAULT NULL,
	cutoff_frequency real DEFAULT NULL,
	operator dsl.es_match_operator DEFAULT NULL,
	auto_generate_synonyms_phrase_query boolean DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query.html

`match` queries accept text/numerics/dates, analyzes them, and constructs a query. 
 
---

#### `dsl.match_all()`

```sql
FUNCTION dsl.match_all (
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html

The most simple query, which matches all documents, giving them all a _score of 1.0.
 
---

#### `dsl.match_none()`

```sql
FUNCTION dsl.match_none ()
RETURNS zdbquery
```

The inverse of `dsl.match_all()`.  Matches no documents.
 
---

#### `dsl.match_phrase()`

```sql
FUNCTION dsl.match_phrase (
	field text,
	query text,
	boost real DEFAULT NULL,
	slop integer DEFAULT NULL,
	analyzer text DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html

The `match_phrase` query analyzes the text and creates a phrase query out of the analyzed text. 
 
---

#### `dsl.match_phrase_prefix()`

```sql
FUNCTION dsl.match_phrase_prefix (
	field text,
	query text,
	boost real DEFAULT NULL,
	slop integer DEFAULT NULL,
	analyzer text DEFAULT NULL,
	max_expansions integer DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html

`ds.match_phrase_prefix()` is the same as `dsl.match_phrase()`, except that it allows for prefix matches on the last term in the text.
 
---

#### `dsl.more_like_this()`

```sql
FUNCTION dsl.more_like_this (
	"like" text,
	fields text[] DEFAULT NULL,
	stop_words text[] DEFAULT ARRAY[...],
	boost real DEFAULT NULL,
	unlike text DEFAULT NULL,
	analyzer text DEFAULT NULL,
	minimum_should_match text DEFAULT NULL,
	boost_terms real DEFAULT NULL,
	include boolean DEFAULT NULL,
	min_term_freq integer DEFAULT NULL,
	max_query_terms integer DEFAULT NULL,
	min_doc_freq integer DEFAULT NULL,
	max_doc_freq integer DEFAULT NULL,
	min_word_length integer DEFAULT NULL,
	max_word_length integer DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html

The More Like This Query finds documents that are "like" a given set of documents. In order to do so, MLT selects a set of representative terms of these input documents, forms a query using these terms, executes the query and returns the results.

This form takes a single blob of text as the source document. 
 
---

#### `dsl.more_like_this()`

```sql
FUNCTION dsl.more_like_this (
	"like" text[],
	fields text[] DEFAULT NULL,
	stop_words text[] DEFAULT ARRAY[...],
	boost real DEFAULT NULL,
	unlike text DEFAULT NULL,
	analyzer text DEFAULT NULL,
	minimum_should_match text DEFAULT NULL,
	boost_terms real DEFAULT NULL,
	include boolean DEFAULT NULL,
	min_term_freq integer DEFAULT NULL,
	max_query_terms integer DEFAULT NULL,
	min_doc_freq integer DEFAULT NULL,
	max_doc_freq integer DEFAULT NULL,
	min_word_length integer DEFAULT NULL,
	max_word_length integer DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html

The More Like This Query finds documents that are "like" a given set of documents. In order to do so, MLT selects a set of representative terms of these input documents, forms a query using these terms, executes the query and returns the results.

This form takes multiple snippets of text as the source documents. 

 
---

#### `dsl.multi_match()`

```sql
FUNCTION dsl.multi_match (
	fields text[],
	query text,
	boost real DEFAULT NULL,
	type dsl.es_multi_match_type DEFAULT NULL,
	analyzer text DEFAULT NULL,
	minimum_should_match text DEFAULT NULL,
	lenient boolean DEFAULT NULL,
	fuzziness integer DEFAULT NULL,
	fuzzy_rewrite text DEFAULT NULL,
	fuzzy_transpositions boolean DEFAULT NULL,
	prefix_length integer DEFAULT NULL,
	zero_terms_query dsl.es_match_zero_terms_query DEFAULT NULL,
	cutoff_frequency real DEFAULT NULL,
	operator dsl.es_match_operator DEFAULT NULL,
	auto_generate_synonyms_phrase_query boolean DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html

The `multi_match` query builds on the match query to allow multi-field queries.
 
---

#### `dsl.query_string()`

```sql
FUNCTION dsl.query_string(
	query text,
	default_operator dsl.esqdsl_default_operators DEFAULT NULL,
	default_field text DEFAULT NULL,
	analyzer text DEFAULT NULL,
	quote_analyzer text DEFAULT NULL,
	allow_leading_wildcard boolean DEFAULT NULL,
	enable_position_increments boolean DEFAULT NULL,
	fuzzy_max_expansions integer DEFAULT NULL,
	fuzziness text DEFAULT NULL,
	fuzzy_prefix_length integer DEFAULT NULL,
	fuzzy_transpositions boolean DEFAULT NULL,
	phrase_slop integer DEFAULT NULL,
	boost real DEFAULT NULL,
	auto_generate_phrase_queries boolean DEFAULT NULL,
	analyze_wildcard boolean DEFAULT NULL,
	max_determinized_states integer DEFAULT NULL,
	minimum_should_match integer DEFAULT NULL,
	lenient boolean DEFAULT NULL,
	time_zone text DEFAULT NULL,
	quote_field_suffix text DEFAULT NULL,
	auto_generate_synonyms_phrase_query boolean DEFAULT NULL,
	all_fields boolean DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

A query that uses a query parser in order to parse its content.  The query_string query parses the input and splits text around operators. Each textual part is analyzed independently of each other.
 
---

#### `dsl.nested()`

```sql
FUNCTION dsl.nested (
	path text,
	query zdbquery,
	score_mode dsl.es_nested_score_mode DEFAULT 'avg'::dsl.es_nested_score_mode)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html

Nested query allows to query nested objects / docs (see nested mapping). The query is executed against the nested objects / docs as if they were indexed as separate docs (they are, internally) and resulting in the root parent doc (or parent nested mapping).
 
---

#### `dsl.noteq()`

```sql
FUNCTION dsl.noteq (
	query zdbquery)
RETURNS zdbquery
```

Generates a `bool` query where the argument is the only member of the `bool` query's `must_not` clause.
 
---

#### `dsl.phrase()`

```sql
FUNCTION dsl.phrase (
	field text,
	query text,
	boost real DEFAULT NULL,
	slop integer DEFAULT NULL,
	analyzer text DEFAULT NULL)
RETURNS zdbquery
```

Short-hand form of `dsl.match_phrase()`.
 
---

#### `dsl.prefix()`

```sql
FUNCTION dsl.prefix (
	field text,
	prefix text,
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html

Matches documents that have fields containing terms with a specified prefix (not analyzed).
 
---

#### `dsl.range()`

```sql
FUNCTION dsl.range (
	field text,
	lt numeric DEFAULT NULL,
	gt numeric DEFAULT NULL,
	lte numeric DEFAULT NULL,
	gte numeric DEFAULT NULL,
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html

Matches documents with fields that have terms within a certain range.  This form is for numeric values.
 
---

#### `dsl.range()`

```sql
FUNCTION dsl.range (
	field text,
	lt text DEFAULT NULL,
	gt text DEFAULT NULL,
	lte text DEFAULT NULL,
	gte text DEFAULT NULL,
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html

Matches documents with fields that have terms within a certain range.  This form is for text values.

 
---

#### `dsl.regexp()`

```sql
FUNCTION dsl.regexp (
	field text,
	regexp text,
	boost real DEFAULT NULL,
	flags dsl.es_regexp_flags[] DEFAULT NULL,
	max_determinized_states integer DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html

The regexp query allows you to use regular expression term queries.
 
---

#### `dsl.script()`

```sql
FUNCTION dsl.script (
	source_code text,
	params json DEFAULT NULL,
	lang text DEFAULT 'painless'::text)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-script-query.html

A query allowing to define scripts as queries. They are typically used in a filter context.
 
---

#### `dsl.span_containing()`

```sql
FUNCTION dsl.span_containing (
	little zdbquery,
	big zdbquery)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-containing-query.html

Returns matches which enclose another span query.
 
---

#### `dsl.span_first()`

```sql
FUNCTION dsl.span_first (
	query zdbquery,
	"end" integer)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-first-query.html

Matches spans near the beginning of a field.
 
---

#### `dsl.span_masking()`

```sql
FUNCTION dsl.span_masking (
	field text,
	query zdbquery)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-field-masking-query.html

Wrapper to allow span queries to participate in composite single-field span queries by lying about their search field.
 
---

#### `dsl.span_multi()`

```sql
FUNCTION dsl.span_multi (
	query zdbquery)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-multi-term-query.html

Allows you to wrap a multi term query (one of `dsl.wildcard()`, `dsl.fuzzy()`, `dsl.prefix()`, `dsl.range()` or `dsl.regexp()` query) as a span query, so it can be nested. 
 
---

#### `dsl.span_near()`

```sql
FUNCTION dsl.span_near (
	in_order boolean,
	slop integer,
	VARIADIC clauses zdbquery[])
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-near-query.html

Matches spans which are near one another. One can specify slop, the maximum number of intervening unmatched positions, as well as whether matches are required to be in-order.
 
---

#### `dsl.span_not()`

```sql
FUNCTION dsl.span_not (
	include zdbquery,
	exclude zdbquery,
	pre integer DEFAULT NULL,
	post integer DEFAULT NULL,
	dist integer DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-not-query.html

Removes matches which overlap with another span query or which are within x tokens before (controlled by the parameter pre) or y tokens after (controled by the parameter post) another SpanQuery.
 
---

#### `dsl.span_or()`

```sql
FUNCTION dsl.span_or (
	VARIADIC clauses zdbquery[])
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-or-query.html

Matches the union of its span clauses.
 
---

#### `dsl.span_term()`

```sql
FUNCTION dsl.span_term (
	field text,
	value text,
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-term-query.html

Matches spans containing a term. 
 
---

#### `dsl.span_within()`

```sql
FUNCTION dsl.span_within (
	little zdbquery,
	big zdbquery)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-within-query.html

Returns matches which are enclosed inside another span query.
 
---

#### `dsl.term()`

```sql
FUNCTION dsl.term (
	field text,
	value numeric,
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html

The term query finds documents that contain the **exact** term specified in the inverted index.  This form is for numeric terms. 
 
---

#### `dsl.term()`

```sql
FUNCTION dsl.term (
	field text,
	value text,
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html

The term query finds documents that contain the **exact** term specified in the inverted index.  This form is for text terms. 

 
---

#### `dsl.terms()`

```sql
FUNCTION dsl.terms (
	field text,
	VARIADIC "values" numeric[])
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html

Filters documents that have fields that match any of the provided terms (not analyzed).  This form is for numeric terms.
 
---

#### `dsl.terms()`

```sql
FUNCTION dsl.terms (
	field text,
	VARIADIC "values" text[])
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html

Filters documents that have fields that match any of the provided terms (not analyzed).  This form is for text terms.
 
---

#### `dsl.terms_array()`

```sql
FUNCTION dsl.terms_array (
	field text,
	"values" anyarray)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html

Filters documents that have fields that match any of the provided terms (not analyzed).  This form is for an array of any kind of Postgres datatype.
 
---

#### `dsl.terms_lookup()`

```sql
FUNCTION dsl.terms_lookup (
	field text,
	index text,
	type text,
	path text,
	id text)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html#query-dsl-terms-lookup

When it’s needed to specify a terms filter with a lot of terms it can be beneficial to fetch those term values from a document in an index.

---

#### `dsl.wildcard()`

```sql
FUNCTION dsl.wildcard (
	field text,
	wildcard text,
	boost real DEFAULT NULL)
RETURNS zdbquery
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html

Matches documents that have fields matching a wildcard expression (not analyzed). Supported wildcards are *, which matches any character sequence (including the empty one), and ?, which matches any single character. Note that this query can be slow, as it needs to iterate over many terms. In order to prevent extremely slow wildcard queries, a wildcard term should not start with one of the wildcards * or ?.
