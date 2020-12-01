use crate::elasticsearch::aggregates::terms::pg_catalog::TermsOrderBy;
use crate::elasticsearch::Elasticsearch;
use crate::utils::{is_date_field, is_string_field, json_to_string};
use crate::zdbquery::ZDBQuery;
use chrono::{TimeZone, Utc};
use pgx::*;
use serde::*;
use serde_json::*;
use std::collections::HashMap;
use std::str::FromStr;

pub(crate) mod pg_catalog {
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
    tally(
        index,
        field_name,
        true,
        None,
        query,
        size_limit,
        order_by,
        Some(std::i32::MAX),
        Some(false),
    )
}

/// ```funcname
/// tally
/// ```
#[pg_extern(immutable, parallel_safe)]
fn tally_not_nested(
    index: PgRelation,
    field_name: &str,
    stem: Option<&str>,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, NULL)>,
    shard_size: Option<default!(i32, 2147483647)>,
    count_nulls: Option<default!(bool, true)>,
) -> impl std::iter::Iterator<Item = (name!(term, Option<String>), name!(count, i64))> {
    tally(
        index,
        field_name,
        false,
        stem,
        query,
        size_limit,
        order_by,
        shard_size,
        count_nulls,
    )
}

#[pg_extern(immutable, parallel_safe)]
fn tally(
    index: PgRelation,
    field_name: &str,
    is_nested: bool,
    stem: Option<&str>,
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
        key_as_string: Option<String>,
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
    struct Terms<'a> {
        field: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        calendar_interval: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        size: Option<i32>,
        min_doc_count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        shard_min_doc_count: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        shard_size: Option<i32>,
        order: Value,
    }

    let is_string_field = is_string_field(&index, field_name);
    let is_raw_date_field = is_date_field(&index, field_name);
    let is_date_field = (stem == Some("year")
        || stem == Some("month")
        || stem == Some("week")
        || stem == Some("day")
        || stem == Some("hour")
        || stem == Some("minute")
        || stem == Some("second"))
        && is_raw_date_field;
    let count_nulls = count_nulls.unwrap_or_default();
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
        field: field_name,
        include: if is_raw_date_field || !is_string_field {
            None
        } else {
            match stem {
                // for backwards compatibility, we strip off a leading ^ as the stem/include
                // isn't a PRCE but a Lucene regex
                Some(stem) if stem.starts_with("^") => Some(stem[1..].to_string()),
                Some(stem) => Some(stem.into()),
                None => None,
            }
        },
        format: if is_date_field {
            Some(match stem {
                Some("year") => "yyyy".into(),
                Some("month") => "yyyy-MM".into(),
                Some("week") => "yyyy-MM-dd".into(),
                Some("day") => "yyyy-MM-dd".into(),
                Some("hour") => "yyyy-MM-dd HH".into(),
                Some("minute") => "yyyy-MM-dd HH:mm".into(),
                Some("second") => "yyyy-MM-dd HH:mm:ss".into(),
                _ => panic!("unrecognized date format"),
            })
        } else {
            None
        },

        calendar_interval: if is_date_field {
            Some(match stem {
                Some("year") => "1y".into(),
                Some("month") => "1M".into(),
                Some("week") => "1w".into(),
                Some("day") => "1d".into(),
                Some("hour") => "1h".into(),
                Some("minute") => "1m".into(),
                Some("second") => "1s".into(),
                _ => panic!("unrecognized date format"),
            })
        } else {
            None
        },
        size: if is_date_field { None } else { size_limit },
        min_doc_count: 1,
        shard_min_doc_count: if is_date_field { None } else { Some(0) },
        shard_size: if is_date_field { None } else { shard_size },
        order,
    };

    let terms_agg_json = if is_date_field {
        json! { { "date_histogram": body } }
    } else {
        json! { { "terms": body } }
    };

    let mut aggregates = HashMap::new();
    aggregates.insert("the_agg".into(), terms_agg_json);

    if count_nulls {
        aggregates.insert(
            "count_nulls".into(),
            json! {
                {
                    "missing": { "field": field_name }
                }
            },
        );
    }

    let (prepared_query, index) = query.prepare(&index, Some(field_name.into()));
    let elasticsearch = Elasticsearch::new(&index);
    let request = elasticsearch.aggregate_set::<TermsAggData>(
        Some(field_name.into()),
        is_nested,
        prepared_query,
        aggregates,
    );

    let (terms, mut others) = request
        .execute_set()
        .expect("failed to execute aggregate search");

    let nulls = if let Some(count_nulls) = others.remove("count_nulls") {
        let count_nulls = serde_json::from_value::<CountNullsAggData>(count_nulls)
            .expect("failed to deserialize count_nulls");
        if count_nulls.doc_count > 0 {
            Some((None, count_nulls.doc_count))
        } else {
            None
        }
    } else {
        None
    };

    nulls
        .into_iter()
        .chain(terms.buckets.into_iter().map(|entry| {
            match entry.key_as_string {
                Some(key) => (Some(key), entry.doc_count),
                None => {
                    let mut key = json_to_string(entry.key);

                    if is_raw_date_field && key.is_some() && !is_date_field {
                        // convert raw date field values into a human-readable string
                        let epoch =
                            i64::from_str(&key.unwrap()).expect("date value not in epoch form");
                        let utc = Utc.timestamp_millis(epoch);
                        key = Some(utc.to_string());
                    }

                    (key, entry.doc_count)
                }
            }
        }))
        .map(|(key, doc_count)| (key, doc_count as i64))
        .collect::<Vec<_>>()
        .into_iter()
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
    field: &str,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, NULL)>,
) -> Vec<Option<String>> {
    terms(index, field, query, size_limit, order_by)
        .map(|(term, _)| term)
        .collect::<Vec<Option<String>>>()
}
