/// This Module is to build...
/// https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics.html
///
/// Returns JsonB for different Metric ES Queries including:
/// Sum: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-sum-aggregation.html
/// Avg: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-avg-aggregation.html
/// Min: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-min-aggregation.html
/// Max: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-max-aggregation.html
/// Stats: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-stats-aggregation.html
/// Cardinality: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-cardinality-aggregation.html
/// Extended Stats: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-extendedstats-aggregation.html
/// Matrix stats: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-matrix-stats-aggregation.html
/// Geo_Bound: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-geobounds-aggregation.html
/// Value_count: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-valuecount-aggregation.html
/// Box_plot: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-boxplot-aggregation.html
/// Geo_Centroid: https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-geocentroid-aggregation.html
/// Median_Absolute_Deviation:https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-metrics-median-absolute-deviation-aggregation.html
///
///
///
use crate::zdbquery::{SortDescriptor, ZDBQuery};
use pgrx::*;
use serde::*;
use serde_json::*;

#[derive(PostgresEnum, Serialize, Deserialize)]
pub enum TTestType {
    Paired,
    Homoscedastic,
    Heteroscedastic,
}

#[pg_extern(immutable, parallel_safe, name = "sum_agg")]
fn sum_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "sum": {
                   "field": field
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "sum_agg")]
fn sum_agg_missing_float(aggregate_name: &str, field: &str, missing: f64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "sum": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "sum_agg")]
fn sum_agg_missing_int(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "sum": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "avg_agg")]
fn avg_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "avg": {
                   "field": field
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "avg_agg")]
fn avg_agg_missing_int(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "avg": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "avg_agg")]
fn avg_agg_missing_float(aggregate_name: &str, field: &str, missing: f64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "avg": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "min_agg")]
fn min_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "min": {
                   "field": field
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "min_agg")]
fn min_agg_missing_int(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "min": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "min_agg")]
fn min_agg_missing_float(aggregate_name: &str, field: &str, missing: f64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "min": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "max_agg")]
fn max_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "max": {
                   "field": field
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "max_agg")]
fn max_agg_missing_int(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "max": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "max_agg")]
fn max_agg_missing_float(aggregate_name: &str, field: &str, missing: f64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "max": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "stats_agg")]
fn stats_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "stats": {
                   "field": field
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "stats_agg")]
fn stats_agg_missing_int(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "stats": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "stats_agg")]
fn stats_agg_missing_float(aggregate_name: &str, field: &str, missing: f64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "stats": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "cardinality_agg")]
fn cardinality_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "cardinality": {
                   "field": field
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "cardinality_agg")]
fn cardinality_agg_missing_int(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "cardinality": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "cardinality_agg")]
fn cardinality_agg_missing_float(aggregate_name: &str, field: &str, missing: f64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "cardinality": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "extended_stats_agg")]
fn extended_stats_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "extended_stats": {
                   "field": field
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "extended_stats_agg")]
fn extended_stats_agg_missing_int(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "extended_stats": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "extended_stats_agg")]
fn extended_stats_agg_missing_float(aggregate_name: &str, field: &str, missing: f64) -> JsonB {
    JsonB(json! {
       {
          aggregate_name: {
                "extended_stats": {
                   "field": field,
                   "missing": missing
                }
            }
       }
    })
}

#[pg_extern(immutable, parallel_safe, name = "matrix_stats_agg")]
fn matrix_stats_agg(aggregate_name: &str, field: Vec<String>) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "matrix_stats": { "fields": field
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "matrix_stats_agg")]
fn matrix_stats_agg_missing_i64(
    aggregate_name: &str,
    field: Vec<String>,
    missing_field: &str,
    missing_value: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "matrix_stats": {
                    "fields": field,
                    "missing": { missing_field: missing_value }
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn geo_bounds_agg(aggregate_name: &str, field: &str, wrap_longitude: bool) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "geo_bounds": {
                    "field": field,
                    "wrap_longitude": wrap_longitude
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn value_count_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "value_count" : {
                    "field" : field
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "boxplot_agg")]
fn boxplot_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "boxplot" : {
                    "field" : field
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "boxplot_agg")]
fn boxplot_missing_agg(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "boxplot" : {
                    "field" : field,
                    "missing" : missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "boxplot_agg")]
fn boxplot_compression_missing_agg(
    aggregate_name: &str,
    field: &str,
    compression: i64,
    missing: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "boxplot" : {
                    "field" : field,
                    "compression" : compression,
                    "missing" : missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn geo_centroid_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "boxplot" : {
                    "field" : field
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "median_absolute_deviation_agg")]
fn median_absolute_deviation_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "median_absolute_deviation" : {
                    "field" : field
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "median_absolute_deviation_agg")]
fn median_absolute_deviation_missing_agg(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "median_absolute_deviation" : {
                    "field" : field,
                    "missing" : missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "median_absolute_deviation_agg")]
fn median_absolute_deviation_compression_missing_agg(
    aggregate_name: &str,
    field: &str,
    compression: i64,
    missing: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "median_absolute_deviation" : {
                    "field" : field,
                    "compression" : compression,
                    "missing" : missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "percentiles_agg")]
fn percentiles_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "percentiles" : {
                    "field" : field
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "percentiles")]
fn percentiles_precents_agg(aggregate_name: &str, field: &str, percents: Vec<f64>) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "percentiles" : {
                    "field" : field,
                    "percents": percents
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "percentiles")]
fn percentiles_keyed_agg(aggregate_name: &str, field: &str, keyed: bool) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "percentiles" : {
                    "field" : field,
                    "keyed": keyed
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "percentiles_agg")]
fn percentiles_missing_agg(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "percentiles" : {
                    "field" : field,
                    "missing": missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "percentiles_agg")]
fn percentiles_precent_keyed_agg(
    aggregate_name: &str,
    field: &str,
    percents: Vec<f64>,
    keyed: bool,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "percentiles" : {
                    "field" : field,
                    "percents": percents,
                    "keyed": keyed
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "percentiles_agg")]
fn percentiles_precent_missing_agg(
    aggregate_name: &str,
    field: &str,
    precents: Vec<f64>,
    missing: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "percentiles" : {
                    "field" : field,
                    "percents": precents,
                    "missing": missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "percentiles_agg")]
fn percentiles_keyed_missing_agg(
    aggregate_name: &str,
    field: &str,
    keyed: bool,
    missing: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "percentiles" : {
                    "field" : field,
                    "keyed": keyed,
                    "missing": missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "percentiles_agg")]
fn percentiles_precents_keyed_missing_agg(
    aggregate_name: &str,
    field: &str,
    percents: Vec<f64>,
    keyed: bool,
    missing: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "percentiles" : {
                    "field" : field,
                    "percents": percents,
                    "keyed": keyed,
                    "missing": missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "string_stats_agg")]
fn string_stats_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name : {
                "message_stats" : {
                    "string_stats" : {
                        "field": field
                    }
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "string_stats_agg")]
fn string_stats_char_distribution_agg(
    aggregate_name: &str,
    field: &str,
    show_distribution: bool,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "string_stats" : {
                    "field": field,
                    "show_distribution": show_distribution
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "string_stats_agg")]
fn string_stats_missing_agg(aggregate_name: &str, field: &str, missing: i64) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "string_stats" : {
                    "field": field,
                    "missing": missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "string_stats_agg")]
fn string_stats_char_distribution_missing_agg(
    aggregate_name: &str,
    field: &str,
    show_distribution: bool,
    missing: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "string_stats" : {
                    "field": field,
                    "show_distribution": show_distribution,
                    "missing": missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "weighted_avg_agg")]
fn weighted_avg_agg(aggregate_name: &str, field_value: &str, field_weight: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "weighted_avg" : {
                    "field": field_value
                },
                "weight": {
                    "field": field_weight
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "weighted_avg_agg")]
fn weighted_avg_missing_value_agg(
    aggregate_name: &str,
    field_value: &str,
    field_weight: &str,
    value_missing: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "weighted_avg" : {
                    "field": field_value,
                    "missing": value_missing
                },
                "weight": {
                    "field": field_weight
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "weighted_avg_agg")]
fn weighted_avg_missings_agg(
    aggregate_name: &str,
    field_value: &str,
    field_weight: &str,
    value_missing: i64,
    weight_missing: i64,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "weighted_avg" : {
                    "field": field_value,
                    "missing": value_missing
                },
                "weight": {
                    "field": field_weight,
                    "missing": weight_missing
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "top_metrics_agg")]
fn top_metric_sort_desc_agg(
    aggregate_name: &str,
    metric_field: &str,
    sort_type: SortDescriptor,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "top_metrics" : {
                    "metrics": {
                        "field": metric_field
                    },
                    "sort" :
                        sort_type
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "top_metrics_agg")]
fn top_metric_score_agg(aggregate_name: &str, metric_field: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "top_metrics" : {
                    "metrics": {
                        "field": metric_field
                    },
                    "sort" : "_score"
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "top_metrics_agg")]
fn top_metric_agg(aggregate_name: &str, metric_field: &str, sort_type_lat_long: Vec<f64>) -> JsonB {
    JsonB(json! {
        {
            aggregate_name :  {
                "top_metrics" : {
                    "metrics": {
                        "field": metric_field
                    },
                    "sort" :{
                        "_geo_distance" : {
                            "location": sort_type_lat_long
                        }
                    }
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "t_test_agg")]
fn t_test_fields_agg(aggregate_name: &str, fields: Vec<String>, t_type: TTestType) -> JsonB {
    if fields.len() > 2 || fields.len() < 2 {
        PANIC!("Wrong amount of fields given, please list only 2")
    }
    JsonB(json! {
        {
            aggregate_name :  {
                "t_test" : {
                    "a": {
                        "field": fields.first().unwrap(),
                     },
                    "b": { "field": fields.last().unwrap() },
                    "type": t_type
                }
            }
        }
    })
}

#[pg_extern(immutable, parallel_safe, name = "t_test_agg")]
fn t_test_fields_queries_agg(
    aggregate_name: &str,
    fields: Vec<String>,
    queries: Vec<ZDBQuery>,
    t_type: TTestType,
) -> JsonB {
    if fields.len() > 2 || fields.len() < 2 {
        PANIC!("Wrong amount of fields given, please list only 2")
    }
    if queries.len() > 2 || queries.len() < 2 {
        PANIC!("Wrong amount of queries given, please list only 2")
    }
    JsonB(json! {
        {
            aggregate_name :  {
                "t_test" : {
                    "a": {
                        "field": fields.first().unwrap(),
                        "filter": queries.first().unwrap()
                     },
                    "b": {
                        "field": fields.last().unwrap(),
                        "filter": queries.last().unwrap()
                        },
                    "type": t_type
                }
            }
        }
    })
}

#[cfg(test)]
#[pgrx::pg_schema]
mod tests {
    use crate::elasticsearch::aggregates::builders::metrics::*;

    #[test]
    fn sum_agg_tests() {
        let output_json = json!(sum_agg("aggregateName", "field"));
        let correct = json! {
            {
                "aggregateName" : {
                    "sum": {
                        "field": "field"
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn sum_agg_test_missing_int() {
        let output_json = json!(sum_agg_missing_int("aggregateName", "field", 10));
        let correct = json! {
            {
                "aggregateName" : {
                    "sum": {
                        "field": "field",
                        "missing": 10
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn sum_agg_test_missing_float() {
        let output_json = json!(sum_agg_missing_float("aggregateName", "field", 15.5));
        let correct = json! {
            {
                "aggregateName" : {
                    "sum": {
                        "field": "field",
                        "missing": 15.5
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn avg_agg_tests() {
        let output_json = json!(avg_agg("aggregateName", "field"));
        let correct = json! {
            {
                "aggregateName" : {
                    "avg": {
                        "field": "field"
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn avg_agg_test_missing_int() {
        let output_json = json!(avg_agg_missing_int("aggregateName", "field", 10));
        let correct = json! {
            {
                "aggregateName" : {
                    "avg": {
                        "field": "field",
                        "missing": 10
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn avg_agg_test_missing() {
        let output_json = json!(avg_agg_missing_float("aggregateName", "field", 15.5));
        let correct = json! {
        {
            "aggregateName" : {
                "avg": {
                    "field": "field",
                    "missing": 15.5
                 }
            }
        }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn min_agg_tests() {
        let output_json = json!(min_agg("aggregateName", "field"));
        let correct = json! {
            {
                "aggregateName" : {
                    "min": {
                        "field": "field"
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn min_agg_test_missing_int() {
        let output_json = json!(min_agg_missing_int("aggregateName", "field", 10));
        let correct = json! {
            {
                "aggregateName" : {
                    "min": {
                        "field": "field",
                        "missing": 10
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn min_agg_test_missing() {
        let output_json = json!(min_agg_missing_float("aggregateName", "field", 15.5));
        let correct = json! {
        {
            "aggregateName" : {
                "min": {
                    "field": "field",
                    "missing": 15.5
                 }
            }
        }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn max_agg_tests() {
        let output_json = json!(max_agg("aggregateName", "field"));
        let correct = json! {
            {
                "aggregateName" : {
                    "max": {
                        "field": "field"
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn max_agg_test_missing_int() {
        let output_json = json!(max_agg_missing_int("aggregateName", "field", 10));
        let correct = json! {
            {
                "aggregateName" : {
                    "max": {
                        "field": "field",
                        "missing": 10
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn max_agg_test_missing() {
        let output_json = json!(max_agg_missing_float("aggregateName", "field", 15.5));
        let correct = json! {
        {
            "aggregateName" : {
                "max": {
                    "field": "field",
                    "missing": 15.5
                 }
            }
        }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn stats_agg_tests() {
        let output_json = json!(stats_agg("aggregateName", "field"));
        let correct = json! {
            {
                "aggregateName" : {
                    "stats": {
                        "field": "field"
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn stats_agg_test_missing_int() {
        let output_json = json!(stats_agg_missing_int("aggregateName", "field", 10));
        let correct = json! {
            {
                "aggregateName" : {
                    "stats": {
                        "field": "field",
                        "missing": 10
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn stats_agg_test_missing() {
        let output_json = json!(stats_agg_missing_float("aggregateName", "field", 15.5));
        let correct = json! {
        {
            "aggregateName" : {
                "stats": {
                    "field": "field",
                    "missing": 15.5
                 }
            }
        }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn cardinality_agg_tests() {
        let output_json = json!(cardinality_agg("aggregateName", "field"));
        let correct = json! {
            {
                "aggregateName" : {
                    "cardinality": {
                        "field": "field"
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn cardinality_agg_test_missing_int() {
        let output_json = json!(cardinality_agg_missing_int("aggregateName", "field", 10));
        let correct = json! {
            {
                "aggregateName" : {
                    "cardinality": {
                        "field": "field",
                        "missing": 10
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn cardinality_agg_test_missing() {
        let output_json = json!(cardinality_agg_missing_float(
            "aggregateName",
            "field",
            15.5
        ));
        let correct = json! {
        {
            "aggregateName" : {
                "cardinality": {
                    "field": "field",
                    "missing": 15.5
                 }
            }
        }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn extended_stats_agg_tests() {
        let output_json = json!(extended_stats_agg("aggregateName", "field"));
        let correct = json! {
            {
                "aggregateName" : {
                    "extended_stats": {
                        "field": "field"
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn extended_stats_agg_test_missing_int() {
        let output_json = json!(extended_stats_agg_missing_int("aggregateName", "field", 10));
        let correct = json! {
            {
                "aggregateName" : {
                    "extended_stats": {
                        "field": "field",
                        "missing": 10
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn extended_stats_agg_test_missing() {
        let output_json = json!(extended_stats_agg_missing_float(
            "aggregateName",
            "field",
            15.5
        ));
        let correct = json! {
        {
            "aggregateName" : {
                "extended_stats": {
                    "field": "field",
                    "missing": 15.5
                 }
            }
        }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn matrix_stats_agg_tests() {
        let output_json = json!(matrix_stats_agg(
            "aggregateName",
            vec!["field_one".into(), "field_two".into()]
        ));
        let correct = json! {
            {
                "aggregateName" : {
                    "matrix_stats": {
                        "fields": ["field_one", "field_two"]
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
    #[test]
    fn matrix_stats_agg_tests_missing_i64() {
        let output_json = json!(matrix_stats_agg_missing_i64(
            "aggregateName",
            vec!["field_one".into(), "field_two".into()],
            "field_one",
            10
        ));
        let correct = json! {
            {
                "aggregateName" : {
                    "matrix_stats": {
                         "fields": [ "field_one", "field_two" ],
                         "missing": { "field_one": 10}
                    }
                }
            }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn geo_bounds_agg_tests() {
        let output_json = json!(geo_bounds_agg("aggregateName", "field_name", true));
        let correct = json! {
            {
                "aggregateName" : {
                    "geo_bounds": {
                        "field": "field_name",
                        "wrap_longitude": true
                     }
                }
            }
        };
        assert_eq!(output_json, correct);
    }

    #[test]
    fn value_count_agg_tests() {
        let output_json = json!(value_count_agg("aggregateName", "field_name"));
        let correct = json! {
            {
                "aggregateName" : {
                    "value_count" : {
                        "field" : "field_name"
                    }
                }
            }
        };
        assert_eq!(output_json, correct);
    }
}
