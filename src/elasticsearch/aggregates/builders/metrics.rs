use pgx::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn sum_agg(aggregate_name: &str, field: &str, missing: default!(i32, 0)) -> JsonB {
    JsonB(json! {
        {
            aggregate_name: {
                "sum": { "field": field, "missing": missing }
            }
        }
    })
}
