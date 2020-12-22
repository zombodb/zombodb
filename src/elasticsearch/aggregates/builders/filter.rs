use crate::elasticsearch::aggregates::builders::make_children_map;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn filter_agg(
    aggregate_name: &str,
    filter: ZDBQuery,
    children: Option<default!(Vec<JsonB>, NULL)>,
) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "filter": filter,
                "aggs": make_children_map(children)
            }
        }
    })
}
