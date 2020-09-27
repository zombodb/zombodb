use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn ip_range(
    index: PgRelation,
    field_name: &str,
    query: ZDBQuery,
    range_array: Json,
) -> impl std::iter::Iterator<
    Item = (
        name!(key, String),
        name!(from, Option<Inet>),
        name!(to, Option<Inet>),
        name!(doc_count, i64),
    ),
> {
    #[derive(Deserialize, Serialize)]
    struct IPRangesAggData {
        buckets: Vec<BucketEntry>,
    }

    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        key: serde_json::Value,
        from: Option<Inet>,
        to: Option<Inet>,
        doc_count: i64,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<IPRangesAggData>(
        query.prepare(&index),
        json! {
            {
                "ip_range": {
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
