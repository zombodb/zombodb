use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn significant_text(
    index: PgRelation,
    field_name: &str,
    query: ZDBQuery,
    sample_size: Option<default!(i32, 0)>,
    filter_duplicate_text: Option<default!(bool, true)>,
) -> impl std::iter::Iterator<
    Item = (
        name!(term, Option<String>),
        name!(doc_count, i64),
        name!(score, f32),
        name!(bg_count, i64),
    ),
> {
    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        doc_count: i64,
        key: serde_json::Value,
        score: f32,
        bg_count: i64,
    }

    #[derive(Deserialize, Serialize)]
    struct TermsAggData {
        buckets: Vec<BucketEntry>,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<TermsAggData>(
        query,
        json! {
            {
                "significant_text": {
                    "field": field_name,
                    "shard_size": std::i32::MAX,
                    "sample_size": sample_size,
                    "filter_duplicate_text": filter_duplicate_text
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.buckets.into_iter().map(|entry| {
        (
            json_to_string(entry.key),
            entry.doc_count,
            entry.score,
            entry.bg_count,
        )
    })
}
