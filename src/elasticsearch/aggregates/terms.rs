use crate::elasticsearch::aggregates::terms::pg_catalog::TermsOrderBy;
use crate::elasticsearch::Elasticsearch;
use crate::utils::{
    is_date_field, is_date_subfield, is_named_index_link, is_string_field, json_to_string,
};
use crate::zdbquery::ZDBQuery;
use chrono::{TimeZone, Utc};
use pgx::*;
use serde::*;
use serde_json::*;
use std::collections::HashMap;
use std::str::FromStr;

#[pgx_macros::pg_schema]
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
    order_by: Option<default!(TermsOrderBy, "'count'")>,
) -> impl std::iter::Iterator<Item = (name!(term, Option<String>), name!(doc_count, i64))> {
    let order_by = match order_by.as_ref().unwrap_or(&TermsOrderBy::count) {
        // we flip the meaning of 'count' and 'reverse_count' here
        // as externally to Postgres we want 'count' to mean ascending and 'reverse_count' to mean descending
        TermsOrderBy::count => TermsOrderBy::reverse_count,
        TermsOrderBy::reverse_count => TermsOrderBy::count,

        // 'term' and 'reverse_term' stay the same
        TermsOrderBy::term | TermsOrderBy::key => TermsOrderBy::key,
        TermsOrderBy::reverse_term | TermsOrderBy::reverse_key => TermsOrderBy::reverse_key,
    };

    tally(
        index,
        field_name,
        true,
        None,
        query,
        size_limit,
        Some(order_by),
        Some(std::i32::MAX),
        Some(false),
    )
}

#[pg_extern(immutable, parallel_safe, name = "tally")]
fn tally_not_nested(
    index: PgRelation,
    field_name: &str,
    stem: Option<&str>,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, "'count'")>,
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
    order_by: Option<default!(TermsOrderBy, "'count'")>,
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

    #[derive(Debug, Serialize)]
    struct Terms {
        field: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        calendar_interval: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        offset: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        size: Option<i32>,
        min_doc_count: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        shard_min_doc_count: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        shard_size: Option<i32>,
        order: Value,
    }

    #[allow(non_camel_case_types)]
    enum DateStem<'a> {
        year(Option<&'a str>),
        month(Option<&'a str>),
        week(Option<&'a str>),
        day(Option<&'a str>),
        houur(Option<&'a str>),
        minute(Option<&'a str>),
        second(Option<&'a str>),
    }

    let original_index = index.clone();
    let (prepared_query, index) = query.prepare(&index, Some(field_name.to_string()));
    let elasticsearch = Elasticsearch::new(&index);

    let field_name = if field_name.contains(".") {
        if is_named_index_link(&original_index, field_name.split('.').next().unwrap()) {
            let mut split = field_name.splitn(2, '.');
            split.next().unwrap();
            split.next().unwrap()
        } else {
            field_name
        }
    } else {
        field_name
    };

    let is_string_field = is_string_field(&index, field_name);
    let is_date_subfield = is_date_subfield(&index, field_name);
    let is_raw_date_field = is_date_field(&index, field_name) || is_date_subfield;

    let date_stem = if is_raw_date_field && stem.is_some() {
        match stem.unwrap() {
            stem if stem.starts_with("year") => Some(DateStem::year(if stem.contains(':') {
                stem.rsplit(':').next()
            } else {
                None
            })),
            stem if stem.starts_with("month") => Some(DateStem::month(if stem.contains(':') {
                stem.rsplit(':').next()
            } else {
                None
            })),
            stem if stem.starts_with("week") => Some(DateStem::week(if stem.contains(':') {
                stem.rsplit(':').next()
            } else {
                None
            })),
            stem if stem.starts_with("day") => Some(DateStem::day(if stem.contains(':') {
                stem.rsplit(':').next()
            } else {
                None
            })),
            stem if stem.starts_with("hour") => Some(DateStem::houur(if stem.contains(':') {
                stem.rsplit(':').next()
            } else {
                None
            })),
            stem if stem.starts_with("minute") => Some(DateStem::minute(if stem.contains(':') {
                stem.rsplit(':').next()
            } else {
                None
            })),
            stem if stem.starts_with("second") => Some(DateStem::second(if stem.contains(':') {
                stem.rsplit(':').next()
            } else {
                None
            })),
            _ => None,
        }
    } else {
        None
    };
    let is_date_field = date_stem.is_some();

    let count_nulls = count_nulls.unwrap_or_default();
    let order = match order_by.unwrap_or(TermsOrderBy::count) {
        // `count` sorts largest to smallest and `reverse_count` sorts smallest to largest
        TermsOrderBy::count => json! {{ "_count": "desc" }},
        TermsOrderBy::reverse_count => json! {{ "_count": "asc" }},
        TermsOrderBy::term | TermsOrderBy::key => json! {{ "_key": "asc" }},
        TermsOrderBy::reverse_term | TermsOrderBy::reverse_key => json! {{ "_key": "desc" }},
    };

    let field_name = if is_date_subfield {
        format!("{}.date", field_name)
    } else {
        field_name.into()
    };
    let body = Terms {
        field: field_name.clone(),
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
        format: match date_stem.as_ref() {
            Some(date_stem) => match date_stem {
                DateStem::year(_) => Some("yyyy".into()),
                DateStem::month(_) => Some("yyyy-MM".into()),
                DateStem::week(_) => Some("yyyy-MM-dd".into()),
                DateStem::day(_) => Some("yyyy-MM-dd".into()),
                DateStem::houur(_) => Some("yyyy-MM-dd HH".into()),
                DateStem::minute(_) => Some("yyyy-MM-dd HH:mm".into()),
                DateStem::second(_) => Some("yyyy-MM-dd HH:mm:ss".into()),
            },
            None => None,
        },

        calendar_interval: match date_stem.as_ref() {
            Some(date_stem) => match date_stem {
                DateStem::year(_) => Some("1y".into()),
                DateStem::month(_) => Some("1M".into()),
                DateStem::week(_) => Some("1w".into()),
                DateStem::day(_) => Some("1d".into()),
                DateStem::houur(_) => Some("1h".into()),
                DateStem::minute(_) => Some("1m".into()),
                DateStem::second(_) => Some("1s".into()),
            },
            None => None,
        },
        offset: match date_stem.as_ref() {
            Some(date_stem) => match date_stem {
                DateStem::year(offset)
                | DateStem::month(offset)
                | DateStem::week(offset)
                | DateStem::day(offset)
                | DateStem::houur(offset)
                | DateStem::minute(offset)
                | DateStem::second(offset) => match offset {
                    Some(offset) => Some(offset.to_string()),
                    None => None,
                },
            },
            None => None,
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
                    "missing": { "field": field_name.clone() }
                }
            },
        );
    }

    let request = elasticsearch.aggregate_set::<TermsAggData>(
        Some(field_name.clone()),
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
#[pg_extern(
    immutable,
    parallel_safe,
    sql = r#"
        CREATE OR REPLACE FUNCTION zdb."terms_array"(
            "index" regclass,
            "field_name" text,
            "query" ZDBQuery,
            "size_limit" integer DEFAULT '2147483647',
            "order_by" TermsOrderBy DEFAULT 'count')
        RETURNS text[]
        IMMUTABLE PARALLEL SAFE
        LANGUAGE c AS 'MODULE_PATHNAME', 'terms_array_agg_wrapper';
    "#
)]
pub(crate) fn terms_array_agg(
    index: PgRelation,
    field: &str,
    query: ZDBQuery,
    size_limit: Option<default!(i32, 2147483647)>,
    order_by: Option<default!(TermsOrderBy, "'count'")>,
) -> Vec<Option<String>> {
    terms(index, field, query, size_limit, order_by)
        .map(|(term, _)| term)
        .collect::<Vec<Option<String>>>()
}
