//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-rare-terms-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct RareTerms<'a> {
    field: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_doc_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    precision: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    include: Option<Vec<&'a str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    exclude: Option<Vec<&'a str>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing: Option<&'a str>,
}

#[pg_extern(immutable, parallel_safe)]
fn rare_terms_agg(
    aggregate_name: &str,
    field: &str,
    max_doc_count: Option<default!(i64, NULL)>,
    precision: Option<default!(f64, NULL)>,
    include: Option<default!(Vec<&str>, NULL)>,
    exclude: Option<default!(Vec<&str>, NULL)>,
    missing: Option<default!(&str, NULL)>,
) -> JsonB {
    let rare_terms = RareTerms {
        field,
        max_doc_count,
        precision,
        include,
        exclude,
        missing,
    };
    JsonB(json! {
        {
            aggregate_name: {
                "rare_terms":
                    rare_terms
            }
        }
    })
}
