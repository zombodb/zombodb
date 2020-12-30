# Aggregate Builder API

The ZDB Aggregate build API is a tool used to build complex aggregate Json that can be used with the Arbitrary Agg function.

Currently we support the following aggregations:

####Metrics
* Sum: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-sum-aggregation.html
* Avg: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-avg-aggregation.html
* Min: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-min-aggregation.html
* Max: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-max-aggregation.html
* Stats: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-stats-aggregation.html
* Cardinality: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-cardinality-aggregation.html
* Extended Stats: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-extendedstats-aggregation.html
* Matrix stats: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-matrix-stats-aggregation.html
* Geo_Bound: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-geobounds-aggregation.html

####Buckets
* Date_Histogram: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-datehistogram-aggregation.html
* Histogram: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-histogram-aggregation.html
* Filter: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-filter-aggregation.html
* Filters: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-filters-aggregation.html
* Range: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-range-aggregation.html
* Term: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-terms-aggregation.html 

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
   (1 row)```