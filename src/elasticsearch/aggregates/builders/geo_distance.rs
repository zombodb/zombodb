//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-geodistance-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct GeoDistance<'a> {
    field: &'a str,
    origin: &'a str,
    range: Vec<Json>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keyed: Option<bool>,
}

#[pg_extern(immutable, parallel_safe)]
fn geo_distance_agg(
    aggregate_name: &str,
    field: &str,
    origin: &str,
    range: Vec<Json>,
    unit: default!(Option<&str>, NULL),
    keyed: default!(Option<bool>, NULL),
) -> JsonB {
    let geo_distance = GeoDistance {
        field,
        origin,
        range,
        unit,
        keyed,
    };
    JsonB(json! {
        {
            aggregate_name: {
                "geo-distance":
                    geo_distance
            }
        }
    })
}
