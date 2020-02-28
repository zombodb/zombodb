use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn significant_terms(
    index: PgRelation,
    field_name: &str,
    query: ZDBQuery,
    include: Option<default!(&str, ".*")>,
    size_limit: Option<default!(i32, 2147483647)>,
    min_doc_count: Option<default!(i32, 3)>,
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
                "significant_terms": {
                    "field": field_name,
                    "include": include,
                    "shard_size": std::i32::MAX,
                    "size": size_limit,
                    "min_doc_count": min_doc_count,
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result.buckets.into_iter().map(|entry| {
        (
            match entry.key {
                Value::Null => None,
                Value::Bool(b) => Some(if b {
                    "true".to_string()
                } else {
                    "false".to_string()
                }),
                Value::Number(n) => Some(n.to_string()),
                Value::String(s) => Some(s),
                _ => panic!("unsupported value type"),
            },
            entry.doc_count,
            entry.score,
            entry.bg_count,
        )
    })
}
