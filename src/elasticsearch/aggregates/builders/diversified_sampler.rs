//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-sampler-aggregation.html
//!
//! Returns JsonB that is a Filer ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use pgx::*;
use serde::*;
use serde_json::*;

#[derive(PostgresEnum, Serialize, Deserialize)]
pub enum ExecutionHint {
    Map,
    GlobalOrdinals,
    BytesHash,
}

#[derive(Serialize)]
struct DiversifiedSampler {
    #[serde(skip_serializing_if = "Option::is_none")]
    shard_size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_docs_per_value: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_hint: Option<ExecutionHint>,
}

#[pg_extern(immutable, parallel_safe)]
fn diversified_sampler_agg(
    aggregate_name: &str,
    shard_size: Option<default!(i64, NULL)>,
    max_docs_per_value: Option<default!(i64, NULL)>,
    execution_hint: Option<default!(ExecutionHint, NULL)>,
    children: Option<default!(Vec<JsonB>, NULL)>,
) -> JsonB {
    let diversified_sampler = DiversifiedSampler {
        shard_size,
        max_docs_per_value,
        execution_hint,
    };

    JsonB(json! {
        {
            aggregate_name: {
                "sampler": diversified_sampler,
                "aggs" : make_children_map(children)
            }
        }
    })
}
