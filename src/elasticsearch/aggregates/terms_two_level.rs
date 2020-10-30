use crate::elasticsearch::aggregates::terms_two_level::pg_catalog::TwoLevelTermsOrderBy;
use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

mod pg_catalog {
    use pgx::*;
    use serde::Serialize;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub(crate) enum TwoLevelTermsOrderBy {
        count,
        term,
        key,
        reverse_count,
        reverse_term,
        reverse_key,
    }
}

#[pg_extern(immutable, parallel_safe)]
fn terms_two_level(
    index: PgRelation,
    field_first: &str,
    field_second: &str,
    query: ZDBQuery,
    order_by: Option<default!(TwoLevelTermsOrderBy, NULL)>,
    size_limit: Option<default!(i32, 2147483647)>,
) -> impl std::iter::Iterator<
    Item = (
        name!(term_one, Option<String>),
        name!(term_two, Option<String>),
        name!(doc_count, i64),
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
        sub_agg: Option<SubAgg>,
    }

    #[derive(Deserialize, Serialize)]
    struct TermsTwoLevel {
        buckets: Vec<BucketEntry>,
    }

    let order = match order_by {
        Some(order_by) => match order_by {
            TwoLevelTermsOrderBy::count => json! {{ "_count": "asc" }},
            TwoLevelTermsOrderBy::term | TwoLevelTermsOrderBy::key => json! {{ "_key": "asc" }},
            TwoLevelTermsOrderBy::reverse_count => json! {{ "_count": "desc" }},
            TwoLevelTermsOrderBy::reverse_term | TwoLevelTermsOrderBy::reverse_key => {
                json! {{ "_key": "desc" }}
            }
        },
        None => {
            json! {{ "_count": "desc" }}
        }
    };

    let (prepared_query, index) = query.prepare(&index, Some(field_first.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate::<TermsTwoLevel>(
        Some(field_first.into()),
        false,
        prepared_query,
        json! {
            {
                "terms": {
                    "field": field_first,
                    "shard_size": std::i32::MAX,
                    "size": size_limit,
                    "order": order
                },
                "aggregations": {
                    "sub_agg": {
                        "terms": {
                            "field": field_second,
                            "shard_size": std::i32::MAX,
                            "size": size_limit,
                            "order": order
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
                ))
            }
        }
    }

    response.into_iter()
}
