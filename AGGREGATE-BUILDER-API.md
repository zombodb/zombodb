# Aggregate Builder API

The ZDB Aggregate build API is a tool used to build complex aggregate Json that can be used with the Arbitrary Agg function.

Currently we support the following aggregations: 
    Metric: Sum, Avg, Min, Max, Stats, Cardinality, Extended Stats, Matrix stats, Geo_Bound
    Buckets: Date_Histogram, Histogram, Filter, Filters, Range, Terms 

## Examples
If you want perform a `sum` aggregation it would look something like this: 
``` 
SELECT * FROM zdb.sum_agg('example_agg', 'fieldname');
```

Which will return

```
                    sum_agg                    
-----------------------------------------------
 {"test_agg": {"sum": {"field": "fieldname"}}}
(1 row)

Time: 18.449 ms
```

 If you do `\df zdb.*_agg` you will notice many of the "metric aggregations" have multiple function 
 signatures. This is to accommodate different forms of the Aggregates.
 
To Continue the Example from above:

 ```
SELECT * FROM zdb.sum_agg('example_agg', 'field_name', 10);
``` 

This will use 10 as the "missing" value producing

```sum_agg                             
   ----------------------------------------------------------------
    {"test_agg": {"sum": {"field": "fieldname", "missing": 10.0}}}
   (1 row)
```

## Function Signatures 

### `sum_agg`
```sql
FUNCTION zdb.sum_agg (
    aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.sum_agg (
    aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.sum_agg (
    aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-sum-aggregation.html
A single-value metrics aggregation that sums up numeric values that are extracted from the aggregated documents. 
 
---

### `avg_agg`
```sql
FUNCTION zdb.avg_agg (
    aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.avg_agg (
    aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.avg_agg (
    aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-avg-aggregation.html
A single-value metrics aggregation that computes the average of numeric values that are extracted from the aggregated documents. 
 
---

### `min_agg`
```sql
FUNCTION zdb.min_agg (
    aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.min_agg (
    aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.min_agg (
    aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-min-aggregation.html
A single-value metrics aggregation that keeps track and returns the minimum value among numeric values extracted from the aggregated documents. 

---

### `max_agg`
```sql
FUNCTION zdb.max_agg (
    aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.max_agg (
    aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.max_agg (
    aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-max-aggregation.html
A single-value metrics aggregation that keeps track and returns the maximum value among the numeric values extracted from the aggregated documents.

---

### `stats_agg`
```sql
FUNCTION zdb.stats_agg (
    aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.stats_agg (
    aggregate_name: text, 
    field: text,
    missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.stats_agg (
    aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-stats-aggregation.html
A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents. 

---

### `cardinality_agg`
```sql
FUNCTION zdb.cardinality_agg (
    aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.cardinality_agg (
    aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.cardinality_agg (
    aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-cardinality-aggregation.html
A single-value metrics aggregation that calculates an approximate count of distinct values.

---

### `extended_stats_agg`
```sql
FUNCTION zdb.extended_stats_agg (
    aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.extended_stats_agg (
    aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.extended_stats_agg (
    aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-extendedstats-aggregation.html
A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents.

---

### `matrix_stats_agg`
```sql
FUNCTION zdb.matrix_stats_agg (
    aggregate_name: text, 
    field: text[]
)
RETURNS jsonb
```
```sql
FUNCTION zdb.matrix_stats_agg (
    aggregate_name: text, 
    field: text[], 
    missing_field: text, 
    missing_value: bigint,
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-matrix-stats-aggregation.html
The matrix_stats aggregation is a numeric aggregation that computes the following statistics over a set of document fields:
count, mean, variance, skewness, kurtosis, covariance, correlation

---

### `geo_bounds_agg`
```sql
FUNCTION zdb.geo_bounds_agg (
    aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.geo_bounds_agg (
    aggregate_name: text, 
    field: text,  
    wrap_longitude: boolean,
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-geobounds-aggregation.html
A metric aggregation that computes the bounding box containing all geo values for a field.

---

### `date_histogram_agg`
```sql
FUNCTION zdb.date_histogram_agg (
    aggregate_name: text, 
    field: text, 
    calendar_interval: calendarinterval DEFAULT NULL::calendarinterval, 
    fixed_interval: text DEFAULT NULL::text, 
    time_zone: text DEFAULT '+00:00'::text, 
    format: text DEFAULT 'yyyy-MM-dd'::text,
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS jsonb
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-datehistogram-aggregation.html
This multi-bucket aggregation is similar to the normal histogram, but it can only be used with date or date range values.

---

### `histogram_agg`
```sql
FUNCTION zdb.histogram_agg (
    aggregate_name: text,
    field: text, 
    "interval": bigint, 
    min_count bigint DEFAULT NULL::bigint, 
    keyed boolean DEFAULT NULL::boolean, 
    missing bigint DEFAULT NULL::bigint, 
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS jsonb
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-histogram-aggregation.html
A multi-bucket values source based aggregation that can be applied on numeric values or numeric range values extracted from the documents.

---

### `filter_agg`
```sql
FUNCTION zdb.filter_agg (
    index: regclass,
    aggregate_name: text,
    field: text,    
    filter: zdbquery, 
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS jsonb
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-filter-aggregation.htmlA multi-bucket values source based aggregation that can be applied on numeric values or numeric range values extracted from the documents.
Defines a single bucket of all the documents in the current document set context that match a specified filter.

---

### `filters_agg`
```sql
FUNCTION zdb.filters_agg (
    index: regclass, 
    aggregate_name: text, 
    labels: text[], 
    filters: zdbquery[]
)
RETURNS jsonb
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-filters-aggregation.html
Defines a multi bucket aggregation where each bucket is associated with a filter.

---

### `range_agg`
```sql
FUNCTION zdb.filters_agg (
    aggregate_name: text, 
    field: text, 
    ranges: json[], 
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS jsonb
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-range-aggregation.html
A multi-bucket value source based aggregation that enables the user to define a set of ranges - each representing a bucket.

---

### `terms_agg`
```sql
FUNCTION zdb.terms_agg (
    aggregate_name: text, 
    field: text, 
    size_limit: integer, 
    order_by: termsorderby 
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS jsonb
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-terms-aggregation.html
A multi-bucket value source based aggregation where buckets are dynamically built - one per unique value.