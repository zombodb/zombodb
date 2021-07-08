//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-daterange-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct IPRange<'a> {
    field: &'a str,
    range: Vec<Json>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keyed: Option<bool>,
}

#[pg_extern(immutable, parallel_safe)]
fn ip_range_agg(
    aggregate_name: &str,
    field: &str,
    range: Vec<Json>,
    keyed: Option<default!(bool, "NULL")>,
) -> JsonB {
    let ip_range = IPRange {
        field,
        range,
        keyed,
    };
    JsonB(json! {
        {
            aggregate_name: {
                "ip_range":
                    ip_range
            }
        }
    })
}
