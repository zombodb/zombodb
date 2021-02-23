//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-significanttext-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct SignificantText<'a> {
    field: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter_duplicate_text: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_doc_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    background_filter: Option<ZDBQuery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    source_fields: Option<Vec<&'a str>>,
}

#[pg_extern(immutable, parallel_safe)]
fn significant_text_agg(
    aggregate_name: &str,
    field: &str,
    filter_duplicate_text: Option<default!(bool, NULL)>,
    min_doc_count: Option<default!(i64, NULL)>,
    size: Option<default!(i64, NULL)>,
    background_filter: Option<default!(ZDBQuery, NULL)>,
    source_fields: Option<default!(Vec<&str>, NULL)>,
) -> JsonB {
    let significant_text = SignificantText {
        field,
        filter_duplicate_text,
        min_doc_count,
        size,
        background_filter,
        source_fields,
    };
    JsonB(json! {
        {
            aggregate_name: {
                "significant_terms":
                    significant_text
            }
        }
    })
}
