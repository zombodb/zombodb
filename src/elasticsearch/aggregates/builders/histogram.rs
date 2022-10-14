//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-histogram-aggregation.html
//!
//! Returns JsonB that is a Histogram ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct Histogram<'a> {
    field: &'a str,
    interval: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keyed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing: Option<i64>,
}

#[pg_extern(immutable, parallel_safe)]
fn histogram_agg(
    aggregate_name: &str,
    field: &str,
    interval: i64,
    min_count: default!(Option<i64>, NULL),
    keyed: default!(Option<bool>, NULL),
    missing: default!(Option<i64>, NULL),
    children: default!(Option<Vec<JsonB>>, NULL),
) -> JsonB {
    let histogram = Histogram {
        field,
        interval,
        min_count,
        keyed,
        missing,
    };

    JsonB(json! {
        {
            aggregate_name: {
                "histogram": histogram,
                "aggs": make_children_map(children)
            }
        }
    })
}
