//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-nested-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn nested_agg(
    aggregate_name: &str,
    path: &str,
    children: Option<default!(Vec<JsonB>, NULL)>,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "nested":{
                    "path": path
                },
                "aggs" : make_children_map(children)
            }
        }
    })
}
