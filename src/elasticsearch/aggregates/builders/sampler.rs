//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-sampler-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn sampler_agg(
    aggregate_name: &str,
    shard_size: i64,
    children: Option<default!(Vec<JsonB>, "NULL")>,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "sampler": {
                    "shard_size": shard_size
                },
                "aggs" : make_children_map(children)
            }
        }
    })
}
