use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::ZDBQuery;
use pgrx::prelude::*;
use pgrx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn date_range(
    index: PgRelation,
    field: &str,
    query: ZDBQuery,
    date_range_array: Json,
) -> TableIterator<(
    name!(key, String),
    name!(from, Option<AnyNumeric>),
    name!(from_as_string, Option<String>),
    name!(to, Option<AnyNumeric>),
    name!(to_as_string, Option<String>),
    name!(doc_count, i64),
)> {
    #[derive(Deserialize, Serialize)]
    struct DateRangesAggData {
        buckets: Vec<BucketEntry>,
    }

    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        key: serde_json::Value,
        from: Option<AnyNumeric>,
        from_as_string: Option<String>,
        to: Option<AnyNumeric>,
        to_as_string: Option<String>,
        doc_count: i64,
    }

    let (prepared_query, index) = query.prepare(&index, Some(field.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<DateRangesAggData>(
        Some(field.into()),
        true,
        prepared_query,
        json! {
            {
                "date_range": {
                    "field": field,
                    "ranges": date_range_array
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    TableIterator::new(result.buckets.into_iter().map(|entry| {
        (
            json_to_string(entry.key).unwrap(),
            entry.from,
            entry.from_as_string,
            entry.to,
            entry.to_as_string,
            entry.doc_count,
        )
    }))
}
