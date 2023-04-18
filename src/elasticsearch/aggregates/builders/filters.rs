//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-filters-aggregation.html
//!
//! Returns JsonB that is a Filers ES Query

use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBQuery;
use pgrx::*;
use serde_json::*;
use std::collections::HashMap;

#[pg_extern(immutable, parallel_safe)]
fn filters_agg(
    index: PgRelation,
    aggregate_name: &str,
    labels: Array<&str>,
    filters: Array<ZDBQuery>,
) -> JsonB {
    let elasticsearch = Elasticsearch::new(&index);

    let mut filters_map = HashMap::new();
    for (label, filter) in labels.iter().zip(filters.iter()) {
        let label = label.expect("NULL labels are not allowed");
        let filter = filter.expect("NULL filters are not allowed");

        filters_map.insert(
            label,
            apply_visibility_clause(&elasticsearch, filter.prepare(&index, None).0, false),
        );
    }

    JsonB(json! {
        {
            aggregate_name: {
                "filters":
                    filters_map

            }
        }
    })
}
