use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn significant_terms_two_level(
    index: PgRelation,
    field_first: &str,
    field_second: &str,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
) -> impl std::iter::Iterator<
    Item = (
        name!(term_one, Option<String>),
        name!(term_two, Option<String>),
        name!(doc_count, i64),
        name!(score, f64),
        name!(bg_count, i64),
        name!(doc_count_error_upper_bound, i64),
        name!(sum_other_doc_count, i64),
    ),
> {
    #[derive(Deserialize, Serialize)]
    struct SubAgg {
        buckets: Vec<BucketEntry>,
    }

    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        doc_count: i64,
        key: serde_json::Value,
        score: Option<f64>,
        bg_count: Option<i64>,
        sub_agg: Option<SubAgg>,
    }

    #[derive(Deserialize, Serialize)]
    struct SignificantTermsTwoLevel {
        doc_count_error_upper_bound: i64,
        sum_other_doc_count: i64,
        buckets: Vec<BucketEntry>,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<SignificantTermsTwoLevel>(
        query,
        json! {
            {
                "terms": {
                    "field": field_first,
                    "shard_size": std::i32::MAX,
                    "size": size_limit
                },
                "aggregations": {
                    "sub_agg": {
                        "significant_terms": {
                            "field": field_second
                        }
                    }
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    let mut response = Vec::new();

    for outer_entry in result.buckets {
        if let Some(sub_agg) = outer_entry.sub_agg {
            for inner_entry in sub_agg.buckets {
                response.push((
                    json_to_string(outer_entry.key.clone()),
                    json_to_string(inner_entry.key),
                    inner_entry.doc_count,
                    inner_entry.score.expect("missing score"),
                    inner_entry.bg_count.expect("missing bg_count"),
                    result.doc_count_error_upper_bound,
                    result.sum_other_doc_count,
                ))
            }
        }
    }

    response.into_iter()
}
