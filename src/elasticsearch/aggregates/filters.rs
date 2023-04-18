use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBQuery;
use pgrx::prelude::*;
use pgrx::*;
use serde::*;
use serde_json::*;
use std::collections::HashMap;

#[pg_extern(immutable, parallel_safe)]
fn filters(
    index: PgRelation,
    labels: Array<&str>,
    filters: Array<ZDBQuery>,
) -> TableIterator<'static, (name!(term, String), name!(doc_count, i64))> {
    let elasticsearch = Elasticsearch::new(&index);

    #[derive(Deserialize, Serialize)]
    struct FilterAggData {
        buckets: HashMap<String, BucketEntry>,
    }

    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        doc_count: i64,
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

    let request = elasticsearch.raw_json_aggregate::<FilterAggData>(json! {
        {
            "filters": {
                "filters": filters_map,
            }
        }
    });

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    TableIterator::new(
        result
            .buckets
            .into_iter()
            .map(|entry| (entry.0, entry.1.doc_count)),
    )
}
