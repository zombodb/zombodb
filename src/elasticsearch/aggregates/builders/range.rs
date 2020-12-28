// use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde_json::*;

// SELECT range_agg('name', 'field', ARRAY[ '{"to": 42}', '{"from":0, "to":99}', '{"from":0}'  ]::json[]);
#[pg_extern(immutable, parallel_safe)]
fn range_agg(aggregate_name: &str, field: &str, ranges: Vec<Json>) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "range":
                {
                    "field": field,
                    "ranges": ranges
                }
            }
        }
    })
}
