use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn range(
    index: PgRelation,
    field_name: &str,
    query: ZDBQuery,
    range_array: Json,
) -> impl std::iter::Iterator<
    Item = (
        name!(key, String),
        name!(from, Option<f32>),
        name!(to, Option<f32>),
        name!(doc_count, i64),
    ),
> {
    #[derive(Deserialize, Serialize)]
    struct RangesAggData {
        buckets: Vec<BucketEntry>,
    }

    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        key: serde_json::Value,
        from: Option<f32>,
        to: Option<f32>,
        doc_count: i64,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<RangesAggData>(
        query,
        json! {
            {
                "range": {
                    "field": field_name,
                    "ranges": range_array
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.buckets.into_iter().map(|entry| {
        (
            json_to_string(entry.key).unwrap(),
            entry.from,
            entry.to,
            entry.doc_count,
        )
    })
}
