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
