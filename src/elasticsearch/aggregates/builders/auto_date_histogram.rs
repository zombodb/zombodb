//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-autodatehistogram-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgrx::*;
use serde::*;
use serde_json::*;

#[derive(PostgresEnum, Serialize, Deserialize)]
pub enum Intervals {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Serialize)]
struct AutoDateHistogram<'a> {
    field: &'a str,
    buckets: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    format: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    minimum_interval: Option<Intervals>,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing: Option<&'a str>,
}

#[pg_extern(immutable, parallel_safe)]
fn auto_date_histogram_agg(
    aggregate_name: &str,
    field: &str,
    buckets: i64,
    format: default!(Option<&str>, NULL),
    minimum_interval: default!(Option<Intervals>, NULL),
    missing: default!(Option<&str>, NULL),
) -> JsonB {
    let adh = AutoDateHistogram {
        field,
        buckets,
        format,
        minimum_interval,
        missing,
    };

    JsonB(json! {
        {
            aggregate_name:  {
                "auto_date_histogram":
                    adh
            }
        }
    })
}
