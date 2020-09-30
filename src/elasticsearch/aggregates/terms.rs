use crate::elasticsearch::aggregates::terms::pg_catalog::TermsOrderBy;
use crate::elasticsearch::Elasticsearch;
use crate::utils::json_to_string;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;
use std::collections::HashMap;

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
    field_name: String,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, NULL)>,
) -> impl std::iter::Iterator<Item = (name!(term, Option<String>), name!(doc_count, i64))> {
    tally(
        index,
        field_name,
        None,
        query,
        size_limit,
        order_by,
        Some(std::i32::MAX),
        Some(false),
    )
}

#[pg_extern(immutable, parallel_safe)]
fn tally(
    index: PgRelation,
    field_name: String,
    stem: Option<String>,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, NULL)>,
    shard_size: Option<default!(i32, 2147483647)>,
    count_nulls: Option<default!(bool, true)>,
) -> impl std::iter::Iterator<Item = (name!(term, Option<String>), name!(count, i64))> {
    #[derive(Deserialize)]
    struct BucketEntry {
        doc_count: u64,
        key: serde_json::Value,
    }

    #[derive(Deserialize)]
    struct TermsAggData {
        buckets: Vec<BucketEntry>,
    }

    #[derive(Deserialize)]
    struct CountNullsAggData {
        doc_count: u64,
    }

    #[derive(Serialize)]
    struct Terms {
        field: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        size: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        shard_size: Option<i32>,
        order: Value,
    }

    let count_nulls = count_nulls.unwrap_or_default();
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

    let body = Terms {
        field: field_name.clone(),
        include: match stem {
            // for backwards compatibility, we strip off a leading ^ as the stem/include
            // isn't a PRCE but a Lucene regex
            Some(stem) if stem.starts_with("^") => Some(stem[1..].to_string()),
            Some(stem) => Some(stem),
            None => None,
        },
        size: size_limit,
        shard_size,
        order,
    };

    let terms_agg_json = json! { { "terms": body } };

    if count_nulls {
        let mut aggregates = HashMap::new();
        aggregates.insert("the_agg".into(), terms_agg_json);
        aggregates.insert(
            "count_nulls".into(),
            json! {
                {
                    "missing": { "field": field_name }
                }
            },
        );
        let request =
            elasticsearch.aggregate_set::<TermsAggData>(query.prepare(&index), aggregates);

        let (terms, mut others) = request
            .execute_set()
            .expect("failed to execute aggregate search");

        let nulls = if let Some(count_nulls) = others.remove("count_nulls") {
            let count_nulls = serde_json::from_value::<CountNullsAggData>(count_nulls)
                .expect("failed to deserialize count_nulls");
            Some((None, count_nulls.doc_count))
        } else {
            None
        };

        nulls
            .into_iter()
            .chain(
                terms
                    .buckets
                    .into_iter()
                    .map(|entry| (json_to_string(entry.key), entry.doc_count)),
            )
            .map(|(key, doc_count)| (key, doc_count as i64))
            .collect::<Vec<_>>()
            .into_iter()
    } else {
        let request =
            elasticsearch.aggregate::<TermsAggData>(query.prepare(&index), terms_agg_json);

        let result = request
            .execute()
            .expect("failed to execute aggregate search");

        result
            .buckets
            .into_iter()
            .map(|entry| (json_to_string(entry.key), entry.doc_count as i64))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

// need to hand-write the DDL for this b/c naming this function "terms_array"
// conflicts with the function of the same name in the 'dsl' module
/// ```sql
/// CREATE OR REPLACE FUNCTION zdb."terms_array"(
///     "index" regclass,
///     "field_name" text,
///     "query" ZDBQuery,
///     "size_limit" integer DEFAULT '2147483647',
///     "order_by" TermsOrderBy DEFAULT NULL)
/// RETURNS text[]
/// IMMUTABLE PARALLEL SAFE
/// LANGUAGE c AS 'MODULE_PATHNAME', 'terms_array_agg_wrapper';
/// ```
#[pg_extern(imutable, parallel_safe)]
fn terms_array_agg(
    index: PgRelation,
    field: String,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, NULL)>,
) -> Vec<Option<String>> {
    terms(index, field, query, size_limit, order_by)
        .map(|(term, _)| term)
        .collect::<Vec<Option<String>>>()
}
