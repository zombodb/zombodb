//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-datehistogram-aggregation.html
//!
//! Returns JsonB that is a Date Histogram ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use crate::elasticsearch::aggregates::date_histogram::pg_catalog::CalendarInterval;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn date_histogram_agg(
    aggregate_name: &str,
    field: &str,
    calendar_interval: Option<default!(CalendarInterval, NULL)>,
    fixed_interval: Option<default!(&str, NULL)>,
    time_zone: default!(&str, "'+00:00'"),
    format: default!(&str, "'yyyy-MM-dd'"),
    children: Option<default!(Vec<JsonB>, NULL)>,
) -> JsonB {
    #[derive(Serialize)]
    struct DateHistogram<'a> {
        field: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        calendar_interval: Option<CalendarInterval>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fixed_interval: Option<&'a str>,
        time_zone: &'a str,
        format: &'a str,
    }

    let date_histogram = DateHistogram {
        field,
        calendar_interval,
        fixed_interval,
        time_zone,
        format,
    };

    if date_histogram.calendar_interval.is_some() && date_histogram.fixed_interval.is_some() {
        error!("Both calendar interval and fixed interval have something. Should be mutually exclusive")
    };

    JsonB(json! {
        {
            aggregate_name: {
                "date_histogram": date_histogram,
                "aggs": make_children_map(children)
            }
        }
    })
}
