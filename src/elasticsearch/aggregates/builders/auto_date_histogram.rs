//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-autodatehistogram-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[derive(PostgresEnum, Serialize, Deserialize)]
pub enum Intervals {
    year,
    month,
    day,
    hour,
    minute,
    second,
}

#[derive(Serialize)]
struct Auto_Date_Histogram<'a> {
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
    format: Option<default!(&str, NULL)>,
    minimum_interval: Option<default!(Intervals, NULL)>,
    missing: Option<default!(&str, NULL)>,
) -> JsonB {
    let adh = Auto_Date_Histogram {
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
