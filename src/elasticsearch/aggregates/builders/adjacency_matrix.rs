//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-adjacency-matrix-aggregation.html
//!
//! Returns JsonB that is a  adjacency matrix ES Query

use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBQuery;
use pgrx::*;
use serde::*;
use serde_json::*;
use std::collections::HashMap;

#[pg_extern(immutable, parallel_safe)]
fn adjacency_matrix_agg(
    index: PgRelation,
    aggregate_name: &str,
    labels: Array<&str>,
    filters: Array<ZDBQuery>,
) -> JsonB {
    let elasticsearch = Elasticsearch::new(&index);

    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        doc_count: i64,
        key: serde_json::Value,
    }

    #[derive(Deserialize, Serialize)]
    struct AdjacencyMatrixAggData {
        buckets: Vec<BucketEntry>,
    }

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
        aggregate_name : {
            "adjacency_matrix": {
                "filters": filters_map
            }
        }
    }
    })
}
