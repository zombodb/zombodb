//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-missing-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn missing_agg(aggregate_name: &str, field: &str) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "missing":{
                    "field": field
                }
            }
        }
    })
}
