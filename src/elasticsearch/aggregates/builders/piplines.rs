//! This Module is to build...
//!
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-avg-bucket-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-bucket-script-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-bucket-selector-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-bucket-sort-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-cumulative-cardinality-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-cumulative-sum-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-derivative-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-extended-stats-bucket-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-inference-bucket-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-max-bucket-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-min-bucket-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-movavg-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-movfn-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-moving-percentiles-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-normalize-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-percentiles-bucket-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-serialdiff-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-stats-bucket-aggregation.html
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-pipeline-sum-bucket-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgx::*;
use serde::*;
use serde_json::*;
use std::collections::HashMap;

#[derive(PostgresEnum, Serialize, Deserialize)]
pub enum GapPolicy {
    Skip,
    InsertZeros,
}
#[derive(PostgresEnum, Serialize, Deserialize)]
pub enum Model {
    Simple,
    Linear,
    Ewma,
    Holt,
    HoltWinters,
}
#[derive(PostgresEnum, Serialize, Deserialize)]
pub enum Method {
    Rescale01,
    Rescale0100,
    PercentOfSum,
    Mean,
    Zscore,
    Softmax,
}

#[pg_extern(immutable, parallel_safe)]
fn avg_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct AvgBucket<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    let bucket = AvgBucket {
        bucket_path,
        gap_policy,
        format,
    };

    JsonB(json! {
       {
         "avg_bucket": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn bucket_script_pipeline_agg(
    script: &str,
    bucket_path_var: Vec<&str>,
    bucket_path_param: Vec<&str>,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct BucketScript<'a> {
        script: &'a str,
        bucket_path: HashMap<&'a str, &'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    if bucket_path_var.len() != bucket_path_param.len() {
        panic!("Not the same amount of bucket path parts given.")
    }
    let mut bucket_path = HashMap::new();
    for (var, param) in bucket_path_var.iter().zip(bucket_path_param.iter()) {
        bucket_path.insert(*var, *param);
    }
    let bucket_script = BucketScript {
        script,
        bucket_path,
        gap_policy,
        format,
    };

    JsonB(json! {
       {
         "bucket_script": bucket_script
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn bucket_selector_pipeline_agg(
    script: &str,
    bucket_path_var: Vec<&str>,
    bucket_path_param: Vec<&str>,
    gap_policy: Option<default!(GapPolicy, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct BucketSelector<'a> {
        script: &'a str,
        bucket_path: HashMap<&'a str, &'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
    }
    if bucket_path_var.len() != bucket_path_param.len() {
        panic!("Not the same amount of bucket path parts given.")
    }
    let mut bucket_path = HashMap::new();
    for (var, param) in bucket_path_var.iter().zip(bucket_path_param.iter()) {
        bucket_path.insert(*var, *param);
    }
    let bucket_selector = BucketSelector {
        script,
        bucket_path,
        gap_policy,
    };

    JsonB(json! {
       {
         "bucket_selector": bucket_selector
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn cumulative_cardinality_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct CumulativeCardinality<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
    }
    let cumulative_cardinality = CumulativeCardinality {
        bucket_path,
        gap_policy,
    };
    JsonB(json! {
       {
         "cumulative_cardinality": cumulative_cardinality
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn cumulative_sum_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct CumulativeSum<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
    }
    let cumulative_sum = CumulativeSum {
        bucket_path,
        gap_policy,
    };
    JsonB(json! {
       {
         "cumulative_sum": cumulative_sum
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn derivative_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct Derivative<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    let bucket = Derivative {
        bucket_path,
        gap_policy,
        format,
    };

    JsonB(json! {
       {
         "derivative": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn extended_stats_bucket_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
    stigma: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct ExtendedStatsBucket<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        stigma: Option<i64>,
    }
    let bucket = ExtendedStatsBucket {
        bucket_path,
        gap_policy,
        format,
        stigma,
    };

    JsonB(json! {
       {
         "extended_stats_bucket": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn inference_pipeline_agg(
    model_id: &str,
    bucket_path: &str,
    inference_config: Option<default!(Json, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct Inference<'a> {
        model_id: &'a str,
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        inference_config: Option<Json>,
    }
    let bucket = Inference {
        model_id,
        bucket_path,
        inference_config,
    };

    JsonB(json! {
       {
         "inference": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn max_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct Max<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    let bucket = Max {
        bucket_path,
        gap_policy,
        format,
    };

    JsonB(json! {
       {
         "max": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn min_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct Min<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    let bucket = Min {
        bucket_path,
        gap_policy,
        format,
    };

    JsonB(json! {
       {
         "min": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn moving_average_pipeline_agg(
    bucket_path: &str,
    model: Option<default!(Model, NULL)>,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    window: Option<default!(i64, NULL)>,
    minimize: Option<default!(bool, NULL)>,
    settings: Option<default!(Json, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct MovingAverage<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        model: Option<Model>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        window: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        minimize: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        settings: Option<Json>,
    }
    let bucket = MovingAverage {
        bucket_path,
        model,
        gap_policy,
        window,
        minimize,
        settings,
    };

    JsonB(json! {
       {
         "moving_avg": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn moving_function_pipeline_agg(
    bucket_path: &str,
    window: i64,
    script: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    shift: Option<default!(&str, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct MovingFunction<'a> {
        bucket_path: &'a str,
        window: i64,
        script: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        shift: Option<&'a str>,
    }
    let bucket = MovingFunction {
        bucket_path,
        window,
        script,
        gap_policy,
        shift,
    };

    JsonB(json! {
       {
         "moving_fn": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn moving_percentiles_pipeline_agg(
    bucket_path: &str,
    window: i64,
    shift: Option<default!(&str, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct MovingPercentiles<'a> {
        bucket_path: &'a str,
        window: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        shift: Option<&'a str>,
    }
    let bucket = MovingPercentiles {
        bucket_path,
        window,
        shift,
    };

    JsonB(json! {
       {
         "moving_percentiles": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn normalize_pipeline_agg(
    bucket_path: &str,
    method: Method,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct Normalize<'a> {
        bucket_path: &'a str,
        method: Method,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    let bucket = Normalize {
        bucket_path,
        method,
        format,
    };

    JsonB(json! {
       {
         "normalize": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn percentiles_bucket_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
    percents: Option<default!(Vec<i64>, NULL)>,
    keyed: Option<default!(bool, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct PercentilesBucket<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        percents: Option<Vec<i64>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        keyed: Option<bool>,
    }
    let bucket = PercentilesBucket {
        bucket_path,
        gap_policy,
        format,
        percents,
        keyed,
    };

    JsonB(json! {
       {
         "percentiles_bucket": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn serial_diff_pipeline_agg(
    bucket_path: &str,
    lag: Option<default!(i64, NULL)>,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct SerialDiff<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        lag: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    let bucket = SerialDiff {
        bucket_path,
        lag,
        gap_policy,
        format,
    };

    JsonB(json! {
       {
         "serial_diff": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn stats_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct Stats<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    let bucket = Stats {
        bucket_path,
        gap_policy,
        format,
    };

    JsonB(json! {
       {
         "stats_bucket": bucket
       }
    })
}

#[pg_extern(immutable, parallel_safe)]
fn sum_pipeline_agg(
    bucket_path: &str,
    gap_policy: Option<default!(GapPolicy, NULL)>,
    format: Option<default!(i64, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct Sum<'a> {
        bucket_path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        gap_policy: Option<GapPolicy>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<i64>,
    }
    let bucket = Sum {
        bucket_path,
        gap_policy,
        format,
    };

    JsonB(json! {
       {
         "sum_bucket": bucket
       }
    })
}
