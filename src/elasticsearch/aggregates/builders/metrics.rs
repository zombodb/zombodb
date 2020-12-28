use pgx::*;
use serde_json::*;

/// ```funcname
/// sum_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// sum_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// sum_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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

/// ```funcname
/// avg_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// avg_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// avg_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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

/// ```funcname
/// min_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// min_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// min_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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

/// ```funcname
/// max_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// max_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// max_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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

/// ```funcname
/// stats_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// stats_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// stats_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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

/// ```funcname
/// cardinality_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// cardinality_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// cardinality_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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

/// ```funcname
/// extended_stats_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// extended_stats_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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
/// ```funcname
/// extended_stats_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
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

/// ```funcname
/// matrix_stats_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
fn matrix_stats_agg(aggregate_name: &str, field: Vec<&str>) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "matrix_stats": { "fields": field
                }
            }
        }
    })
}
/// ```funcname
/// matrix_stats_agg
/// ```
#[pg_extern(immutable, parallel_safe)]
fn matrix_stats_agg_missing_i64(
    aggregate_name: &str,
    field: Vec<&str>,
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

/// ```funcname
/// geo_bounds_agg
/// ```
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

/// ```funcname
/// geo_bounds_agg
/// ```
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

#[cfg(test)]
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
            vec!["field_one", "field_two"]
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
            vec!["field_one", "field_two"],
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
