//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-daterange-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgx::*;
use serde::*;
use serde_json::*;

#[derive(Serialize)]
struct GeohashGrid<'a> {
    field: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    precision: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bounds: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_size: Option<i64>,
}

#[pg_extern(immutable, parallel_safe)]
fn geohash_grid_agg(
    aggregate_name: &str,
    field: &str,
    precision: Option<default!(i16, NULL)>,
    bounds: Option<default!(&str, NULL)>,
    size: Option<default!(i64, NULL)>,
    shard_size: Option<default!(i64, NULL)>,
) -> JsonB {
    let geohash_grid = GeohashGrid {
        field,
        precision,
        bounds,
        size,
        shard_size,
    };
    JsonB(json! {
        {
            aggregate_name: {
                "geohash_grid":
                    geohash_grid
            }
        }
    })
}
