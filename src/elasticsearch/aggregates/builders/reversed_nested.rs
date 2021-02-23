//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-reverse-nested-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct ReverseNested<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<&'a str>,
}

#[pg_extern(immutable, parallel_safe)]
fn reverse_nested_agg(
    aggregate_name: &str,
    path: Option<default!(&str, NULL)>,
    children: Option<default!(Vec<JsonB>, NULL)>,
) -> JsonB {
    let reverse_nested = ReverseNested { path };

    JsonB(json! {
        {
            aggregate_name: {
                "reverse_nested":
                    reverse_nested,
                "aggs" : make_children_map(children)
            }
        }
    })
}
