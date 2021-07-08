//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-daterange-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct DateRange<'a> {
    field: &'a str,
    format: &'a str,
    range: Vec<Json>,
    #[serde(skip_serializing_if = "Option::is_none")]
    missing: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keyed: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    time_zone: Option<&'a str>,
}

#[pg_extern(immutable, parallel_safe)]
fn date_range_agg(
    aggregate_name: &str,
    field: &str,
    format: &str,
    range: Vec<Json>,
    missing: Option<default!(&str, "NULL")>,
    keyed: Option<default!(bool, "NULL")>,
    time_zone: Option<default!(&str, "NULL")>,
) -> JsonB {
    let date_range = DateRange {
        field,
        format,
        range,
        missing,
        keyed,
        time_zone,
    };
    JsonB(json! {
        {
            aggregate_name: {
                "date_range":
                    date_range
            }
        }
    })
}
