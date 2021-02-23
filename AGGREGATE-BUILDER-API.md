# Aggregate Builder API

The ZDB Aggregate build API is a tool used to build complex aggregate Json that can be used with the Arbitrary Agg function.

Currently we support the following aggregations: 
    Metric: Sum, Avg, Min, Max, Stats, Cardinality, Extended Stats, Matrix stats, Geo_Bound, Box_plot, Geo_Centroid, Median_Absolute_Deviation, Percentiles, String_Stats, Weighted_Avg, Top_Metric, T_Test, Value_Count
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
FUNCTION zdb.children_agg (
    aggregate_name: text,
    join_type: text, 
    children: jsonb[] DEFAULT NULL::jsonb[],
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-children-aggregation.html
A special single bucket aggregation that selects child documents that have the specified type, as defined in a join field.

---
## `sampler_agg`
```sql
FUNCTION zdb.sampler_agg (
    aggregate_name: text,
    shard_size: bigint, 
    children: jsonb[] DEFAULT NULL::jsonb[],
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-sampler-aggregation.html
A filtering aggregation used to limit any sub aggregations' processing to a sample of the top-scoring documents.

---
## `diversified_sampler_agg`
```sql
FUNCTION zdb.diversified_sampler_agg (
    aggregate_name: text,
    shard_size: bigint,
    max_docs_per_value bigint DEFAULT NULL::bigint,
    execution_hint zdb.executionhint DEFAULT NULL::zdb.executionhint,
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-diversified-sampler-aggregation.html#_max_docs_per_value
Like the sampler aggregation this is a filtering aggregation used to limit any sub aggregations' processing to a sample of the top-scoring documents. The diversified_sampler aggregation adds the ability to limit the number of matches that share a common value such as an "author".

---
## `date_range_agg`
```sql
FUNCTION zdb.date_range_agg (
    aggregate_name: text,
    field: text,
    format: text,
    range json[], 
    missing text DEFAULT NULL::text, 
    keyed boolean DEFAULT NULL::boolean, 
    time_zone text DEFAULT NULL::text
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-daterange-aggregation.html
A range aggregation that is dedicated for date values. The main difference between this aggregation and the normal range aggregation is that the from and to values can be expressed in Date Math expressions, and it is also possible to specify a date format by which the from and to response fields will be returned.

---
## `geo_distance_agg`
```sql
FUNCTION zdb.geo_distance_agg (
    aggregate_name: text,
    field: bigint,
    origin text, 
    range json[],
    unit text DEFAULT NULL::text,
    keyed boolean DEFAULT NULL::boolean
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-geodistance-aggregation.html
A multi-bucket aggregation that works on geo_point fields and conceptually works very similar to the range aggregation.

---
## `geohash_grid_agg`
```sql
FUNCTION zdb.geohash_grid_agg (
    aggregate_name: text,
    field: bigint,
    "precision" smallint DEFAULT NULL::smallint,
    bounds text DEFAULT NULL::text,
    size bigint DEFAULT NULL::bigint,
    shard_size bigint DEFAULT NULL::bigint
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-geohashgrid-aggregation.html
A multi-bucket aggregation that works on geo_point fields and groups points into buckets that represent cells in a grid.

---
## `geotile_grid_agg`
```sql
FUNCTION zdb.geohash_grid_agg (
    aggregate_name: text,
    field: bigint,
    "precision" smallint DEFAULT NULL::smallint,
    bounds text DEFAULT NULL::text,
    size bigint DEFAULT NULL::bigint,
    shard_size bigint DEFAULT NULL::bigint
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-geotilegrid-aggregation.html
A multi-bucket aggregation that works on geo_point fields and groups points into buckets that represent cells in a grid. 


---
## `global_agg`
```sql
FUNCTION zdb.global_agg (
    aggregate_name: text,
    children: jsonb[] DEFAULT NULL::jsonb[],
)
RETURNS JsonB
``` 
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-global-aggregation.html
Defines a single bucket of all the documents within the search execution context. This context is defined by the indices and the document types you’re searching on, but is not influenced by the search query itself.

---
## `ip_range_agg`
```sql
FUNCTION zdb.ip_range_agg (
    aggregate_name: text,
    field: text,
    format: text,
    range json[], 
    missing text DEFAULT NULL::text, 
    keyed boolean DEFAULT NULL::boolean, 
    time_zone text DEFAULT NULL::text
)
RETURNS JsonB
``` 
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-iprange-aggregation.html
Just like the dedicated date range aggregation, there is also a dedicated range aggregation for IP typed fields.

---
## `missing_agg`
```sql
FUNCTION zdb.missing_agg (
    aggregate_name: text,
    field: text,
)
RETURNS JsonB
``` 
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-missing-aggregation.html
A field data based single bucket aggregation, that creates a bucket of all documents in the current document set context that are missing a field value (effectively, missing a field or having the configured NULL value set).

---
## `nested_agg`
```sql
FUNCTION zdb.nested_agg (
    aggregate_name: text,
    field: text,
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS JsonB
``` 
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-nested-aggregation.html
A special single bucket aggregation that enables aggregating nested documents.

---
## `parent_agg`
```sql
FUNCTION zdb.parent_agg (
    aggregate_name: text,
    type_: text,
    children: jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS JsonB
``` 
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-parent-aggregation.html
A special single bucket aggregation that selects parent documents that have the specified type, as defined in a join field.

---
## `rare_terms_agg`
```sql
FUNCTION zdb.parent_agg (
    aggregate_name text, 
    field text, 
    max_doc_count bigint DEFAULT NULL::bigint, 
    "precision" double precision DEFAULT NULL::double precision, 
    include text[] DEFAULT NULL::text[], 
    exclude text[] DEFAULT NULL::text[], 
    missing text DEFAULT NULL::text
)
RETURNS JsonB
``` 
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-rare-terms-aggregation.html
A multi-bucket value source based aggregation which finds "rare" terms — terms that are at the long-tail of the distribution and are not frequent.

---
## `reverse_nested_agg`
```sql
FUNCTION zdb.reverse_nested_agg (
    aggregate_name text,    
    path text DEFAULT NULL::text, 
    children jsonb[] DEFAULT NULL::jsonb[]
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-reverse-nested-aggregation.html
A special single bucket aggregation that enables aggregating on parent docs from nested documents.


---
## `significant_terms_agg`
```sql
FUNCTION zdb.significant_terms_agg (
    aggregate_name text, 
    field text, 
    min_doc_count bigint DEFAULT NULL::bigint, 
    size bigint DEFAULT NULL::bigint,
    background_filter zdbquery DEFAULT NULL::zdbquery
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-significantterms-aggregation.html
An aggregation that returns interesting or unusual occurrences of terms in a set.

---
## `significant_text_agg`
```sql
FUNCTION zdb.significant_text_agg (
    aggregate_name text, 
    field text, 
    filter_duplicate_text boolean DEFAULT NULL::boolean,
    min_doc_count bigint DEFAULT NULL::bigint, 
    size bigint DEFAULT NULL::bigint,
    background_filter zdbquery DEFAULT NULL::zdbquery,
    source_fields text[] DEFAULT NULL::text[]
)
RETURNS JsonB
```
https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-significanttext-aggregation.html
An aggregation that returns interesting or unusual occurrences of free-text terms in a set.

---
## `variable_width_histogram_agg`
```sql
FUNCTION zdb.variable_width_histogram_agg (
    aggregate_name text, 
    field text, 
    bucket bigint DEFAULT NULL::bigint, 
    shard_size bigint DEFAULT NULL::bigint, 
    initial_buffer bigint DEFAULT NULL::bigint
)
RETURNS JsonB
```

https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-variablewidthhistogram-aggregation.html
This is a multi-bucket aggregation similar to Histogram. However, the width of each bucket is not specified.


