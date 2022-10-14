//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-children-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn children_agg(
    aggregate_name: &str,
    join_type: &str,
    children: default!(Option<Vec<JsonB>>, NULL),
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "children": {
                    "type": join_type
                },
                "aggs" : make_children_map(children)
            }
        }
    })
}
