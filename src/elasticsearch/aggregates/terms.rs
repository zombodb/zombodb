use crate::elasticsearch::aggregates::terms::pg_catalog::TermsOrderBy;
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
    pub(crate) enum TermsOrderBy {
        count,
        term,
        key,
        reverse_count,
        reverse_term,
        reverse_key,
    }
}

#[pg_extern(immutable, parallel_safe)]
fn terms(
    index: PgRelation,
    field_name: &str,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, NULL)>,
) -> impl std::iter::Iterator<Item = (name!(term, Option<String>), name!(doc_count, i64))> {
    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        doc_count: i64,
        key: serde_json::Value,
    }

    #[derive(Deserialize, Serialize)]
    struct TermsAggData {
        buckets: Vec<BucketEntry>,
    }

    let elasticsearch = Elasticsearch::new(&index);
    let order = match order_by {
        Some(order_by) => match order_by {
            TermsOrderBy::count => json! {{ "_count": "asc" }},
            TermsOrderBy::term | TermsOrderBy::key => json! {{ "_key": "asc" }},
            TermsOrderBy::reverse_count => json! {{ "_count": "desc" }},
            TermsOrderBy::reverse_term | TermsOrderBy::reverse_key => json! {{ "_key": "desc" }},
        },
        None => {
            json! {{ "_count": "desc" }}
        }
    };

    let request = elasticsearch.aggregate::<TermsAggData>(
        query,
        json! {
            {
                "terms": {
                    "field": field_name,
                    "shard_size": std::i32::MAX,
                    "size": size_limit,
                    "order": order
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    result
        .buckets
        .into_iter()
        .map(|entry| (json_to_string(entry.key), entry.doc_count))
}
