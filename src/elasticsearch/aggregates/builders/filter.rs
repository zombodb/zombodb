//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-filter-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBQuery;
use pgrx::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn filter_agg(
    index: PgRelation,
    aggregate_name: &str,
    filter: ZDBQuery,
    children: default!(Option<Vec<JsonB>>, NULL),
) -> JsonB {
    let elasticsearch = Elasticsearch::new(&index);

    let json_filter =
        apply_visibility_clause(&elasticsearch, filter.prepare(&index, None).0, false);

    JsonB(json! {
        {
            aggregate_name: {
                "filter": json_filter,
                "aggs": make_children_map(children)
            }
        }
    })
}
