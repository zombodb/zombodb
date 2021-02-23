//! This Module is to build...
//!https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-variablewidthhistogram-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct VariableWidthHistogram<'a> {
    field: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    initial_buffer: Option<i64>,
}

#[pg_extern(immutable, parallel_safe)]
fn variable_width_histogram_agg(
    aggregate_name: &str,
    field: &str,
    bucket: Option<default!(i64, NULL)>,
    shard_size: Option<default!(i64, NULL)>,
    initial_buffer: Option<default!(i64, NULL)>,
) -> JsonB {
    let variable_width_histogram = VariableWidthHistogram {
        field,
        bucket,
        shard_size,
        initial_buffer,
    };

    JsonB(json! {
        {
            aggregate_name: {
                "variable_width_histogram":
                    variable_width_histogram
            }
        }
    })
}
