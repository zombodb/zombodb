//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-range-aggregation.html
//!
//! Returns JsonB that is a Range ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde_json::*;

// SELECT range_agg('name', 'field', ARRAY[ '{"to": 42}', '{"from":0, "to":99}', '{"from":0}'  ]::json[]);
#[pg_extern(immutable, parallel_safe)]
fn range_agg(
    aggregate_name: &str,
    field: &str,
    ranges: Vec<Json>,
    children: Option<default!(Vec<JsonB>, "NULL")>,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "range":
                {
                    "field": field,
                    "ranges": ranges
                },
                "aggs": make_children_map(children)
            }
        }
    })
}
