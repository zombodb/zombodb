use crate::elasticsearch::aggregates::terms::pg_catalog::TermsOrderBy;
use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;
use std::iter::FromIterator;

mod pg_catalog {
    use pgx::*;
    use serde::Serialize;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub(crate) enum TermsOrderBy {
        count,
        term,
        reverse_count,
        reverse_term,
    }
}

#[pg_extern]
fn terms(
    index: PgRelation,
    field_name: &str,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, NULL)>,
) -> impl std::iter::Iterator<Item = (name!(term, String), name!(doc_count, i64))> {
    #[derive(Deserialize, Serialize)]
    struct BucketEntry {
        doc_count: i64,
        key: serde_json::Value,
    }

    #[derive(Deserialize, Serialize)]
    struct TermsAggData {
        buckets: Vec<BucketEntry>,
    }

    let heaprel = index.get_heap_relation().expect("index is not an index");
    let elasticsearch = Elasticsearch::new(&heaprel, &index);

    // TODO:  how to deal with the order by?

    let request = elasticsearch.aggregate::<TermsAggData>(
        query,
        json! {
            {
                "terms": {
                    "field": field_name,
                    "shard_size": std::i32::MAX,
                    "size": size_limit
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
        .map(|entry| (entry.key.to_string(), entry.doc_count))
}
