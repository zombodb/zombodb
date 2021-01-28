# Aggregate Builder API

The ZDB Aggregate build API is a tool used to build complex aggregate Json that can be used with the Arbitrary Agg function.

Currently we support the following aggregations: 
    Metric: Sum, Avg, Min, Max, Stats, Cardinality, Extended Stats, Matrix stats, Geo_Bound, Box_plot, Geo_Centroid, Median_Absolute_Deviation, Percentiles, String_Stats, Weighted_Avg, Top_Metric, T_Test, Value_Count
    Buckets: Date_Histogram, Histogram, Filter, Filters, Range, Terms 

###Example
If you want to do a query for the Sum it would look something like this: 
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
 If you do `\df zdb.*_agg` you will notice with the Metric Aggs have multiple function signatures. This is to accommodate different forms of the Aggregates.
 To Continue the Example form above:
 ```
SELECT * FROM zdb.sum_agg('example_agg', 'field_name', 10);
``` 
This will use 10 as the "missing" value producing 
```sum_agg                             
   ----------------------------------------------------------------
    {"test_agg": {"sum": {"field": "fieldname", "missing": 10.0}}}
   (1 row)
```

###Function Signatures 
## `sum_agg`
```sql
FUNCTION zdb.sum_agg (
	aggregate_name: text, 
    field: text
)
RETURNS JsonB
```
```sql
FUNCTION zdb.sum_agg (
	aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS JsonB
```
```sql
FUNCTION zdb.sum_agg (
	aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-sum-aggregation.html
A single-value metrics aggregation that sums up numeric values that are extracted from the aggregated documents. 
 
---

## `avg_agg`
```sql
FUNCTION zdb.avg_agg (
	aggregate_name: text, 
    field: text
)
RETURNS JsonB
```
```sql
FUNCTION zdb.avg_agg (
	aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS JsonB
```
```sql
FUNCTION zdb.avg_agg (
	aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-avg-aggregation.html
A single-value metrics aggregation that computes the average of numeric values that are extracted from the aggregated documents. 
 
---

## `min_agg`
```sql
FUNCTION zdb.min_agg (
	aggregate_name: text, 
    field: text
)
RETURNS JsonB
```
```sql
FUNCTION zdb.min_agg (
	aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS JsonB
```
```sql
FUNCTION zdb.min_agg (
	aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-min-aggregation.html
A single-value metrics aggregation that keeps track and returns the minimum value among numeric values extracted from the aggregated documents. 

---

## `max_agg`
```sql
FUNCTION zdb.max_agg (
	aggregate_name: text, 
    field: text
)
RETURNS JsonB
```
```sql
FUNCTION zdb.max_agg (
	aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS JsonB
```
```sql
FUNCTION zdb.max_agg (
	aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-max-aggregation.html
A single-value metrics aggregation that keeps track and returns the maximum value among the numeric values extracted from the aggregated documents.

---
## `stats_agg`
```sql
FUNCTION zdb.stats_agg (
	aggregate_name: text, 
    field: text
)
RETURNS JsonB
```
```sql
FUNCTION zdb.stats_agg (
	aggregate_name: text, 
    field: text,
    missing: bigint,
)
RETURNS JsonB
```
```sql
FUNCTION zdb.stats_agg (
	aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-stats-aggregation.html
A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents. 

---
## `cardinality_agg`
```sql
FUNCTION zdb.cardinality_agg (
	aggregate_name: text, 
    field: text
)
RETURNS JsonB
```
```sql
FUNCTION zdb.cardinality_agg (
	aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS JsonB
```
```sql
FUNCTION zdb.cardinality_agg (
	aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-cardinality-aggregation.html
A single-value metrics aggregation that calculates an approximate count of distinct values.

---
## `extended_stats_agg`
```sql
FUNCTION zdb.extended_stats_agg (
	aggregate_name: text, 
    field: text
)
RETURNS JsonB
```
```sql
FUNCTION zdb.extended_stats_agg (
	aggregate_name: text, 
    field: text, 
    missing: bigint,
)
RETURNS JsonB
```
```sql
FUNCTION zdb.extended_stats_agg (
	aggregate_name: text, 
    field: text,  
    missing: double precision
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-extendedstats-aggregation.html
A multi-value metrics aggregation that computes stats over numeric values extracted from the aggregated documents.

---
## `matrix_stats_agg`
```sql
FUNCTION zdb.matrix_stats_agg (
	aggregate_name: text, 
    field: text[]
)
RETURNS JsonB
```
```sql
FUNCTION zdb.matrix_stats_agg (
	aggregate_name: text, 
    field: text[], 
    missing_field: text, 
    missing_value: bigint,
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-matrix-stats-aggregation.html
The matrix_stats aggregation is a numeric aggregation that computes the following statistics over a set of document fields:
count, mean, variance, skewness, kurtosis, covariance, correlation

---
## `geo_bounds_agg`
```sql
FUNCTION zdb.geo_bounds_agg (
	aggregate_name: text, 
    field: text
)
RETURNS JsonB
```
```sql
FUNCTION zdb.geo_bounds_agg (
	aggregate_name: text, 
    field: text,  
    wrap_longitude: boolean,
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-geobounds-aggregation.html
A metric aggregation that computes the bounding box containing all geo values for a field.

---
### `box_plot_agg`
```sql
FUNCTION zdb.box_plot_agg (
	aggregate_name: text, 
    field: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.box_plot_agg (
	aggregate_name: text, 
    field: text,  
    compression: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.box_plot_agg (
	aggregate_name: text, 
    field: text,  
    missing: bigint
)
RETURNS jsonb
```
```sql
FUNCTION zdb.box_plot_agg (
	aggregate_name: text, 
    field: text,  
    compression: text,
    missing: bigint
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-boxplot-aggregation.html
A boxplot metrics aggregation that computes boxplot of numeric values extracted from the aggregated documents.

---
### `geo_centroid_agg`
```sql
FUNCTION zdb.geo_centroid_agg (
	aggregate_name: text, 
    field: text,
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-geocentroid-aggregation.html
A metric aggregation that computes the weighted centroid from all coordinate values for geo fields.

---
### `median_absolute_deviation_agg`
```sql
FUNCTION zdb.median_absolute_deviation_agg (
	aggregate_name: text, 
    field: text,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.median_absolute_deviation_agg (
	aggregate_name: text, 
    field: text,  
    compression: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.median_absolute_deviation_agg (
	aggregate_name: text, 
    field: text,  
    missing: bigint
)
RETURNS jsonb
```
```sql
FUNCTION zdb.median_absolute_deviation_agg (
	aggregate_name: text, 
    field: text,  
    compression: text,
    missing: bigint
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-median-absolute-deviation-aggregation.html
This single-value aggregation approximates the median absolute deviation of its search results.

---
### `percentiles_agg`
```sql
FUNCTION zdb.percentiles_agg (
	aggregate_name: text, 
    field: text,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.percentiles_agg (
	aggregate_name: text, 
    field: text,
    percents: double precision[],
)
RETURNS jsonb
```
```sql
FUNCTION zdb.percentiles_agg (
	aggregate_name: text, 
    field: text,
    missing: bigint
)
RETURNS jsonb
```

```sql
FUNCTION zdb.percentiles_agg (
	aggregate_name: text, 
    field: text,
    keyed: boolean
)
RETURNS jsonb
```
```sql
FUNCTION zdb.percentiles_agg (
	aggregate_name: text, 
    field: text,
    percents: double precision[],
    keyed: boolean
)
RETURNS jsonb
```
```sql
FUNCTION zdb.percentiles_agg (
	aggregate_name: text, 
    field: text,
    percents: double precision[],
    missing: bigint
)
RETURNS jsonb
```
```sql
FUNCTION zdb.percentiles_agg (
	aggregate_name: text, 
    field: text,
    keyed: boolean,
    missing: bigint
)
RETURNS jsonb
```
```sql
FUNCTION zdb.percentiles_agg (
	aggregate_name: text, 
    field: text,
    percents: double precision[],
    keyed: boolean,
    missing: bigint
)
RETURNS jsonb
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-percentile-aggregation.html#search-aggregations-metrics-percentile-aggregation
A multi-value metrics aggregation that calculates one or more percentiles over numeric values extracted from the aggregated documents.
By default, the percentile metric will generate a range of percentiles: [ 1, 5, 25, 50, 75, 95, 99 ].

---
### `string_stats_agg`
```sql
FUNCTION zdb.string_stats_agg (
	aggregate_name: text, 
    field: text,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.string_stats_agg (
	aggregate_name: text, 
    field: text,
    show_distribution: boolean
)
RETURNS jsonb
```
```sql
FUNCTION zdb.string_stats_agg (
	aggregate_name: text, 
    field: text,
    missing: bigint
)
RETURNS jsonb
```
```sql
FUNCTION zdb.string_stats_agg (
	aggregate_name: text, 
    field: text,
    show_distribution: boolean,
    missing: bigint
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-string-stats-aggregation.html
A multi-value metrics aggregation that computes statistics over string values extracted from the aggregated documents.

---
### `weighted_avg_agg`
```sql
FUNCTION zdb.weighted_avg_agg (
	aggregate_name: text,
    field_value: text,
    field_weight: text
)
RETURNS jsonb
```
```sql
FUNCTION zdb.weighted_avg_agg (
	aggregate_name: text, 
    field_value: text,
    field_weight: text,
    weight_missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.weighted_avg_agg (
	aggregate_name: text, 
    field_value: text,
    field_weight: text,
    value_missing: bigint,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.weighted_avg_agg (
	aggregate_name: text, 
    field_value: text,
    field_weight: text,
    value_missing: bigint,
    weight_missing: bigint,
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-weight-avg-aggregation.html
A single-value metrics aggregation that computes the weighted average of numeric values that are extracted from the aggregated documents.

---
### `top_metrics_agg`
```sql
FUNCTION zdb.top_metrics_agg (
	aggregate_name: text,
    metric_field: text,
    sort_type: SortDescriptor
)
RETURNS jsonb
```
```sql
FUNCTION zdb.top_metrics_agg (
	aggregate_name: text, 
    metric_field: text,
)
RETURNS jsonb
```
```sql
FUNCTION zdb.top_metrics_agg (
	aggregate_name: text, 
    metric_field: text,
    sort_type_lat_long: double precision[]
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-top-metrics.html
The top_metrics aggregation selects metrics from the document with the largest or smallest "sort" value.

---
### `t_test_agg`
```sql
FUNCTION zdb.t_test_agg (
	aggregate_name: text,
    field: text[],
    t_type: TTestType
)
RETURNS jsonb
```
```sql
FUNCTION zdb.t_test_agg (
	aggregate_name: text, 
    field: text[],
    queries: ZDBQuery[],
    t_type: TTestType
)
RETURNS jsonb
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-ttest-aggregation.html
A t_test metrics aggregation that performs a statistical hypothesis test in which the test statistic follows a Student’s t-distribution under the null hypothesis on numeric values extracted from the aggregated documents or generated by provided scripts. 
In practice, this will tell you if the difference between two population means are statistically significant and did not occur by chance alone.


---
## `date_histogram_agg`
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
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-datehistogram-aggregation.html
This multi-bucket aggregation is similar to the normal histogram, but it can only be used with date or date range values.

---
## `histogram_agg`
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
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-histogram-aggregation.html
A multi-bucket values source based aggregation that can be applied on numeric values or numeric range values extracted from the documents.

---
## `filter_agg`
```sql
FUNCTION zdb.filter_agg (
    index: regclass,
    aggregate_name: text,
    field: text,    
    filter: zdbquery, 
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-filter-aggregation.htmlA multi-bucket values source based aggregation that can be applied on numeric values or numeric range values extracted from the documents.
Defines a single bucket of all the documents in the current document set context that match a specified filter.

---
## `filters_agg`
```sql
FUNCTION zdb.filters_agg (
    index: regclass, 
    aggregate_name: text, 
    labels: text[], 
    filters: zdbquery[]
)
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-filters-aggregation.html
Defines a multi bucket aggregation where each bucket is associated with a filter.

---
## `range_agg`
```sql
FUNCTION zdb.filters_agg (
    aggregate_name: text, 
    field: text, 
    ranges: json[], 
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-range-aggregation.html
A multi-bucket value source based aggregation that enables the user to define a set of ranges - each representing a bucket.

---
## `terms_agg`
```sql
FUNCTION zdb.terms_agg (
    aggregate_name: text, 
    field: text, 
    size_limit: integer, 
    order_by: termsorderby 
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-terms-aggregation.html
A multi-bucket value source based aggregation where buckets are dynamically built - one per unique value.

---
## `adjacency_matrix_agg`
```sql
FUNCTION zdb.adjacency_matrix_agg (
    index: regclass, 
    aggregate_name: text,
    labels: text[], 
    filters: zdbquery[]
)
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-adjacency-matrix-aggregation.html#search-aggregations-bucket-adjacency-matrix-aggregation
A bucket aggregation returning a form of adjacency matrix. The request provides a collection of named filter expressions, similar to the filters aggregation request. 

---
## `adjacency_matrix_agg`
```sql
FUNCTION zdb.auto_date_histogram_agg (
    aggregate_name: text,
    buckets: bigint, 
    format: text DEFAULT NULL::text,
    minimum_interval: Interval DEFAULT NULL::Interval,
    missing: text DEFAULT NULL::text,
)
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-autodatehistogram-aggregation.html
A multi-bucket aggregation similar to the Date histogram except instead of providing an interval to use as the width of each bucket, a target number of buckets is provided indicating the number of buckets needed and the interval of the buckets is automatically chosen to best achieve that target.

---
## `children_agg`
```sql
FUNCTION zdb.auto_date_histogram_agg (
    aggregate_name: text,
    join_type: text, 
    children: jsonb[] DEFAULT NULL::jsonb[],
)
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-children-aggregation.html
A special single bucket aggregation that selects child documents that have the specified type, as defined in a join field.