//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-parent-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn parent_agg(
    aggregate_name: &str,
    type_: &str,
    children: Option<default!(Vec<JsonB>, NULL)>,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "parent":{
                    "type": type_
                },
                "aggs" : make_children_map(children)
            }
        }
    })
}
