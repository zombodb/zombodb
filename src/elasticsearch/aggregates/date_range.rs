use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn date_range(
    index: PgRelation,
    field_name: &str,
    query: ZDBQuery,
    date_range_array: Json,
) -> impl std::iter::Iterator<
    Item = (
        name!(key, String),
        name!(from, Option<f32>),
        name!(from_as_string, Option<String>),
        name!(to, Option<f32>),
        name!(to_as_string, Option<String>),
        name!(doc_count, i64),
    ),
> {
    #[derive(Deserialize, Serialize)]
    struct DateRangesAggData {
        buckets: Vec<BucketEntry>,
    }

    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        key: serde_json::Value,
        from: Option<f32>,
        from_as_string: Option<String>,
        to: Option<f32>,
        to_as_string: Option<String>,
        doc_count: i64,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<DateRangesAggData>(
        query,
        json! {
            {
                "date_range": {
                    "field": field_name,
                    "ranges": date_range_array
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
            entry.from_as_string,
            entry.to,
            entry.to_as_string,
            entry.doc_count,
        )
    })
}
