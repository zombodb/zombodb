use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;
use std::collections::HashMap;

#[pg_extern(immutable, parallel_safe)]
fn filters(
    index: PgRelation,
    labels: Array<&str>,
    filters: Array<ZDBQuery>,
) -> impl std::iter::Iterator<Item = (name!(term, String), name!(doc_count, i64))> {
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
        let label = label.unwrap_or_else(|| panic!("NULL labels are not allowed"));
        let filter = filter.unwrap_or_else(|| panic!("NULL filters are not allowed"));

        filters_map.insert(
            label,
            filter
                .query_dsl()
                .unwrap_or_else(|| panic!("ZDBQuery doesn't have a query_dsl"))
                .clone(),
        );
    }

    let elasticsearch = Elasticsearch::new(&index);

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

    result
        .buckets
        .into_iter()
        .map(|entry| (entry.0, entry.1.doc_count))
}
