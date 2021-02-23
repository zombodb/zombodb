//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-daterange-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct SignificantTerms<'a> {
    field: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_doc_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    background_filter: Option<ZDBQuery>,
}

#[pg_extern(immutable, parallel_safe)]
fn significant_terms_agg(
    aggregate_name: &str,
    field: &str,
    min_doc_count: Option<default!(i64, NULL)>,
    size: Option<default!(i64, NULL)>,
    background_filter: Option<default!(ZDBQuery, NULL)>,
) -> JsonB {
    let significant_term = SignificantTerms {
        field,
        min_doc_count,
        size,
        background_filter,
    };
    JsonB(json! {
        {
            aggregate_name: {
                "significant_terms":
                    significant_term
            }
        }
    })
}
