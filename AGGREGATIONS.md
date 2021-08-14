# Aggregate Functions

ZomboDB exposes nearly all of Elasticsearch's aggregates as type-checked SQL functions that return tables and discreet
values, as opposed to json blobs.

In all cases, unless explicitly otherwise noted, the results returned from all of the below aggregate functions are
MVCC-correct. This means that the functions only operate against records that are considered visible to the current
transaction.

## Arbitrary Aggregate Support

```sql
FUNCTION zdb.arbitrary_agg(
	index regclass,
	query zdbquery,
	agg_json json) 
RETURNS json
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html

This function is a direct-path for executing any arbitrary aggregate search request that Elasticsearch supports.

The result is a json blob that can be processed in your application code or otherwise manipulated using Postgres json
support functions.

## Single-Value Aggregates

```sql
FUNCTION zdb.avg(
	index regclass,
	field text,
	query zdbquery) 
RETURNS numeric
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html

A single-value metrics aggregation that computes the average of numeric values that are extracted from the aggregated
documents. These values can be extracted either from specific numeric fields in the documents.

______________________________________________________________________

```sql
FUNCTION zdb.cardinality(
	index regclass,
	field text,
	query zdbquery) 
RETURNS numeric
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-cardinality-aggregation.html

A single-value metrics aggregation that calculates an approximate count of distinct values. Values can be extracted
either from specific fields in the document.

______________________________________________________________________

```sql
FUNCTION zdb.count(
	index regclass,
	query zdbquery) 
RETURNS bigint
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-count.html

Not an aggregate per se, this function is mapped to Elasticsearch's `_count` endpoint and simply returns the number of
documents that match the provided query. The result is MVCC-correct.

______________________________________________________________________

```sql
FUNCTION zdb.raw_count(
	index regclass,
	query zdbquery) 
RETURNS bigint SET zdb.ignore_visibility = true
```

Similar to `zdb.count()` above, but it ignores MVCC visibility rules, and the result is the actual count of documents
matching the query, including deleted documents, documents from aborted transactions, old versions of documents from an
UPDATE statement, and new/updated docs from in-flight transactions.

Generally you'll want to use `zdb.count()` instead.

______________________________________________________________________

```sql
FUNCTION zdb.max(
	index regclass,
	field text,
	query zdbquery) 
RETURNS numeric
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-max-aggregation.html

A single-value metrics aggregation that keeps track and returns the maximum value among the numeric values extracted
from the aggregated documents. These values can be extracted either from specific numeric fields in the documents.

______________________________________________________________________

```sql
FUNCTION zdb.min(
	index regclass,
	field text,
	query zdbquery) 
RETURNS numeric
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html

A single-value metrics aggregation that keeps track and returns the minimum value among numeric values extracted from
the aggregated documents. These values can be extracted either from specific numeric fields in the documents.

______________________________________________________________________

```sql
FUNCTION zdb.missing(
	index regclass,
	field text,
	query zdbquery) 
RETURNS numeric
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-missing-aggregation.html

A field data based single bucket aggregation, that creates a bucket of all documents in the current document set context
that are missing a field value (effectively, missing a field or having the configured NULL value set).

______________________________________________________________________

```sql
FUNCTION zdb.sum(
	index regclass,
	field text,
	query zdbquery) 
RETURNS numeric
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html

A single-value metrics aggregation that sums up numeric values that are extracted from the aggregated documents. These
values can be extracted either from specific numeric fields in the documents.

______________________________________________________________________

```sql
FUNCTION zdb.value_count(
	index regclass,
	field text,
	query zdbquery) 
RETURNS numeric
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-valuecount-aggregation.html

A single-value metrics aggregation that counts the number of values that are extracted from the aggregated documents.
These values can be extracted either from specific fields in the documents.

______________________________________________________________________

## Multi-Row/Column Aggregates

The following aggregates transform the results from Elasticsearch into "tables", and should all be queried as such. For
example:

```sql
SELECT * FROM zdb.terms('idxproducts', 'tags', dsl.match_all());
```

______________________________________________________________________

```sql
FUNCTION zdb.adjacency_matrix(
	index regclass,
	labels text[],
	filters zdbquery[]) 
RETURNS TABLE (
	key text,
	doc_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-adjacency-matrix-aggregation.html

A bucket aggregation returning a form of adjacency matrix. The request provides a collection of named filter
expressions, similar to the filters aggregation request. Each bucket in the response represents a non-empty cell in the
matrix of intersecting filters.

The `labels` and `filters` arguments must have the same bounds.

______________________________________________________________________

```sql
FUNCTION zdb.adjacency_matrix_2x2(
	index regclass,
	labels text[],
	filters zdbquery[]) 
RETURNS TABLE (
	"-" text,
	"1" text,
	"2" text)
```

This is a table-based version of `zdb.adjacency_matrix()` that outputs a 2x2 matrix.

The `labels` and `filters` arguments must have the same bounds.

______________________________________________________________________

```sql
FUNCTION zdb.adjacency_matrix_3x3(
	index regclass,
	labels text[],
	filters zdbquery[]) 
RETURNS TABLE (
	"-" text,
	"1" text,
	"2" text,
	"3" text)
```

This is a table-based version of `zdb.adjacency_matrix()` that outputs a 3x3 matrix.

The `labels` and `filters` arguments must have the same bounds.

______________________________________________________________________

```sql
FUNCTION zdb.adjacency_matrix_4x4(
	index regclass,
	labels text[],
	filters zdbquery[]) 
RETURNS TABLE (
	"-" text,
	"1" text,
	"2" text,
	"3" text,
	"4" text)
```

This is a table-based version of `zdb.adjacency_matrix()` that outputs a 4x4 matrix.

The `labels` and `filters` arguments must have the same bounds.

______________________________________________________________________

```sql
FUNCTION zdb.adjacency_matrix_5x5(
	index regclass,
	labels text[],
	filters zdbquery[]) 
RETURNS TABLE (
	"-" text,
	"1" text,
	"2" text,
	"3" text,
	"4" text,
	"5" text)
```

This is a table-based version of `zdb.adjacency_matrix()` that outputs a 5x5 matrix.

The `labels` and `filters` arguments must have the same bounds.

______________________________________________________________________

```sql
FUNCTION zdb.date_histogram(
	index regclass,
	field text,
	query zdbquery,
	"interval" text,
	format text DEFAULT 'yyyy-MM-dd') 
RETURNS TABLE (
	key numeric,
	key_as_string text,
	doc_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-datehistogram-aggregation.html

A multi-bucket aggregation similar to the histogram except it can only be applied on date values. Since dates are
represented in Elasticsearch internally as long values, it is possible to use the normal histogram on dates as well,
though accuracy will be compromised. The reason for this is in the fact that time based intervals are not fixed (think
of leap years and on the number of days in a month). For this reason, we need special support for time based data. From
a functionality perspective, this histogram supports the same features as the normal histogram. The main difference is
that the interval can be specified by date/time expressions.

______________________________________________________________________

```sql
FUNCTION zdb.date_range(
	index regclass,
	field text,
	query zdbquery,
	date_ranges_array json) 
RETURNS TABLE (
	key text,
	"from" numeric,
	from_as_string timestamp with time zone,
	"to" numeric,
	to_as_string timestamp with time zone,
	doc_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html

A range aggregation that is dedicated for date values. The main difference between this aggregation and the normal range
aggregation is that the from and to values can be expressed in Date Math expressions, and it is also possible to specify
a date format by which the from and to response fields will be returned. Note that this aggregation includes the from
value and excludes the to value for each range.

______________________________________________________________________

```sql
FUNCTION zdb.extended_stats(
	index regclass,
	field text,
	query zdbquery,
	sigma int DEFAULT 0) 
RETURNS TABLE (
	count bigint,
	min numeric,
	max numeric,
	avg numeric,
	sum numeric,
	sum_of_squares numeric,
	variance numeric,
	stddev numeric,
	stddev_upper numeric,
	stddev_lower numeric)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-extendedstats-aggregation.html

A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents. These
values can be extracted either from specific numeric fields in the documents.

______________________________________________________________________

```sql
FUNCTION zdb.filters(
	index regclass,
	labels text[],
	filters zdbquery[]) 
RETURNS TABLE (
	label text,
	doc_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-filters-aggregation.html

Similar to `zdb.count()` except multiple queries (filters) are supported.

The `labels` and `filters` arguments must have the same bounds.

______________________________________________________________________

```sql
FUNCTION zdb.histogram(
	index regclass,
	field text,
	query zdbquery,
	"interval" float8) 
RETURNS TABLE (
	key numeric,
	doc_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-histogram-aggregation.html

A multi-bucket values source based aggregation that can be applied on numeric values extracted from the documents. It
dynamically builds fixed size (a.k.a. interval) buckets over the values. For example, if the documents have a field that
holds a price (numeric), we can configure this aggregation to dynamically build buckets with interval 5 (in case of
price it may represent $5).

______________________________________________________________________

```sql
FUNCTION zdb.ip_range(
	index regclass,
	field text,
	query zdbquery,
	ip_ranges_array json) 
RETURNS TABLE (
	key text,
	"from" inet,
	"to" inet,
	doc_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-iprange-aggregation.html

Just like the dedicated date range aggregation, there is also a dedicated range aggregation for IP typed fields.

______________________________________________________________________

```sql
FUNCTION zdb.matrix_stats(
	index regclass,
	fields text[],
	query zdbquery) 
RETURNS TABLE (
	name text,
	count bigint,
	mean numeric,
	variance numeric,
	skewness numeric,
	kurtosis numeric,
	covariance json,
	correlation json)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-matrix-stats-aggregation.html

The matrix_stats aggregation is a numeric aggregation that computes various statistics over a set of document fields.

______________________________________________________________________

```sql
FUNCTION zdb.percentile_ranks(
	index regclass,
	field text,
	query zdbquery,
	"values" text DEFAULT '') 
RETURNS TABLE (
	percentile numeric,
	value numeric)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-rank-aggregation.html

A multi-value metrics aggregation that calculates one or more percentile ranks over numeric values extracted from the
aggregated documents. These values can be extracted either from specific numeric fields in the documents.

______________________________________________________________________

```sql
FUNCTION zdb.percentiles(
	index regclass,
	field text,
	query zdbquery,
	percents text DEFAULT '') 
RETURNS TABLE (
	percentile numeric,
	value numeric)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-percentile-aggregation.html

A multi-value metrics aggregation that calculates one or more percentiles over numeric values extracted from the
aggregated documents. These values can be extracted either from specific numeric fields in the documents.

______________________________________________________________________

```sql
FUNCTION zdb.range(
	index regclass,
	field text,
	query zdbquery,
	ranges_array json) 
RETURNS TABLE (
	key text,
	"from" numeric,
	"to" numeric,
	doc_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html

A multi-bucket value source based aggregation that enables the user to define a set of ranges - each representing a
bucket. During the aggregation process, the values extracted from each document will be checked against each bucket
range and "bucket" the relevant/matching document. Note that this aggregation includes the from value and excludes the
to value for each range.

______________________________________________________________________

```sql
FUNCTION zdb.significant_terms(
    index regclass, 
    field text, 
    query zdbquery, 
    include text DEFAULT '.*'::text, 
    size_limit integer DEFAULT 2147483647, 
    min_doc_count integer DEFAULT 3)
RETURNS TABLE (
	term text,
	doc_count bigint,
	score numeric,
	bg_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significantterms-aggregation.html

An aggregation that returns interesting or unusual occurrences of terms in a set.

______________________________________________________________________

```sql
FUNCTION zdb.significant_terms_two_level(
	index regclass,
	first_field text,
	second_field text,
	query zdbquery,
	size bigint DEFAULT 0) 
RETURNS TABLE (
	first_term text,
	second_term text,
	doc_count bigint,
	score numeric,
	bg_count bigint,
	doc_count_error_upper_bound bigint,
	sum_other_doc_count bigint)
```

An adaption of `zdb.significant_terms()` where it uses `zdb.terms()` for the terms from `first_field` and
`zdb.significant_terms()` for the terms for `second_field`.

______________________________________________________________________

```sql
FUNCTION zdb.significant_text(
	index regclass,
	field text,
	query zdbquery,
	sample_size int DEFAULT 0,
	filter_duplicate_text boolean DEFAULT true) 
RETURNS TABLE (
	term text,
	doc_count bigint,
	score numeric,
	bg_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significanttext-aggregation.html

An aggregation that returns interesting or unusual occurrences of free-text terms in a set. It is like the significant
terms aggregation but differs in that:

- It is specifically designed for use on type text fields
- It does not require field data or doc-values
- It re-analyzes text content on-the-fly meaning it can also filter duplicate sections of noisy text that otherwise tend
  to skew statistics.

This aggregate is only supported by Elasticsearch 6+ clusters.

______________________________________________________________________

```sql
FUNCTION zdb.suggest_terms(
    index regclass,
    field_name text,
    suggest test,
    query zdbquery,
) RETURNS TABLE (
        term text,
        offset bigint,
        length bigint,
        suggestion text,
        score double precision,
        frequency bigint,

)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-suggesters.html

While not necessarily an aggregate, `zdb.terms_suggester` will tokenize an input textual suggestion string and provide
suggestions for each token that contains suggestions.

Useful for correcting misspellings -- ie, "Did you mean?"-style queries

______________________________________________________________________

```sql
FUNCTION zdb.stats(
	index regclass,
	field text,
	query zdbquery) 
RETURNS TABLE (
	count bigint,
	min numeric,
	max numeric,
	avg numeric,
	sum numeric)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html

A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents. These
values can be extracted either from specific numeric fields in the documents.

______________________________________________________________________

```sql
FUNCTION zdb.tally(
    index regclass, 
    field_name text,
    [ is_nested bool],
    stem text, 
    query ZDBQuery, 
    size_limit integer DEFAULT '2147483647', 
    order_by TermsOrderBy DEFAULT 'count', 
    shard_size integer DEFAULT '2147483647', 
    count_nulls bool DEFAULT 'true'
) RETURNS TABLE (term text, count bigint)
```

`index`: The name of the a ZomboDB index to query\
`field)_name`: The name of a field from which to derive
terms\
`is_nested`: Optional argument to indicate that the terms should only come from matching nested object
sub-elements. Default is `false`\
`stem`: a Regular expression by which to filter returned terms, or a date interval if
the specified `fieldname` is a date or timestamp\
`query`: a ZomboDB query\
`size_limit`: maximum number of terms to
return. A NULL value means "all terms". `order_by`: how to sort the results. one of `'count'` (descending), `'term'`,
`'reverse_count'` (ascending), `'reverse_term'`\
`shard_size`: optional parameter that tells Elasticsearch how many
terms to return from each shard. Default is zero, which means all terms\
`count_nulls`: should a row containing the
count of NULL (ie, missing) values be included in the results?

This function provides direct access to Elasticsearch's
[terms aggregate](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html)
and cannot be used with fields of type `fulltext`. The results are MVCC-safe. Returned terms are forced to upper-case.

If a stem is not specified, no results will be returned. To match all terms, use a stem of `^.*`

The `order_by` parameter defaults to `count`, which sorts documents by the occurrence count, largest to smallest. A
value of `reverse_count` will sort them smallest to largest.

If the specified `fieldname` is a date/timestamp, then one of the following values are allowed for aggregating values
into histogram buckets of the specified interval: `year, quarter, month, week, day, hour, minute, second`. In all cases,
an optional offset value can be specified. For example: `week:-1d` will offset the dates by one day so that the first
day of the week will be considered to be Sunday (instead of the default of Monday).

Example:

```sql
SELECT * FROM zdb.tally('products', 'keywords', '^.*', 'base* or distance', 5000, 'term');

    term      | count 
>---------------+-------
BASEBALL      |     1
COMMUNICATION |     1
PRIMITIVE     |     1
SPORTS        |     1
THOMAS EDISON |     1
```

Regarding the `is_nested` argument, consider data like this:

```
row #1: contributor_data=[ 
  { "name": "John Doe", "age": 42, "location": "TX", "tags": ["active"] },
  { "name": "Jane Doe", "age": 36, "location": "TX", "tags": ["nice"] }
>]
>
>row #2: contributor_data=[ 
  { "name": "Bob Dole", "age": 92, "location": "KS", "tags": ["nice", "politician"] },
  { "name": "Elizabth Dole", "age": 79, "location": "KS", "tags": ["nice"] }
>]
```

And a query where `is_nested` is false:

```sql
SELECT * FROM zdb.tally('idxproducts', 'contributor_data.name', false, '^.*', 'contributor_data.location:TX AND contributor_data.tags:nice', 5000, 'term');
```

returns:

```
    term   | count 
----------+-------
  JANE DOE |     1
  JOHN DOE |     1
(2 rows)
```

> Whereas, if `is_nested` is true, only "JANE DOE" is returned because it's the only subelement of `contributor_data`
> that matched the query:

```sql
SELECT * FROM zdb.tally('idxproducts', 'contributor_data.name', true, '^.*', 'contributor_data.location:TX WITH contributor_data.tags:nice', 5000, 'term');
```

returns:

```
    term   | count 
----------+-------
  JANE DOE |     1
(1 row)
```

```sql
CREATE TYPE TermsOrderBy AS ENUM (
	'count',
	'term',
	'reverse_count',
	'reverse_term');

FUNCTION zdb.terms(
	index regclass,
	field text,
	query zdbquery,
	size_limit bigint DEFAULT 0,
	order_by TermsOrderBy DEFAULT 'count') 
RETURNS TABLE (
	term text,
	doc_count bigint)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-terms-aggregation.html

A multi-bucket value source based aggregation where buckets are dynamically built - one per unique value.

Note that the `order_by` argument defines how to sort the results:

- `'count'` (ascending),
- `'reverse_count'` (ascending),
- `'term'` (ascending),
- `'reverse_term'` (descending)

______________________________________________________________________

```sql
FUNCTION zdb.terms_array(
	index regclass,
	field text,
	query zdbquery,
	size_limit bigint DEFAULT 0,
	order_by TermsOrderBy DEFAULT 'count') 
RETURNS text[]
```

A version of `zdb.terms()` that instead returns only the terms as a `text[]`.

______________________________________________________________________

```sql
FUNCTION zdb.terms_two_level(
	index regclass,
	first_field text,
	second_field text,
	query zdbquery,
	order_by TwoLevelTermsOrderBy DEFAULT 'count',
	size bigint DEFAULT 0) 
RETURNS TABLE (
	first_term text,
	second_term text,
	doc_count bigint)
```

Similar to `zdb.significant_terms_two_level()`, this is an adaption of `zdb.terms()` to provide a two-level nested
hierarchy of terms from two different fields.

______________________________________________________________________

```sql
FUNCTION zdb.top_hits(
	index regclass,
	fields text[],
	query zdbquery,
	size int) 
RETURNS TABLE (
	ctid tid,
	score float4,
	source json)
```

https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-top-hits-aggregation.html

A top_hits metric aggregator keeps track of the most relevant document being aggregated. This aggregator is intended to
be used as a sub aggregator, so that the top matching documents can be aggregated per bucket.

______________________________________________________________________

```sql
FUNCTION zdb.top_hits_with_id(
	index regclass,
	fields text[],
	query zdbquery,
	size int) 
RETURNS TABLE (
	_id text,
	score float4,
	source json)
```

Similar to `zdb.top_hits()` above, but returns the Elasticsearch document `_id` value for each hit rather than the
corresponding Postgres `ctid` value.
