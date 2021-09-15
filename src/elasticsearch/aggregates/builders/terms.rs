//! This Module is to build...
//! https://www.elastic.co/guide/en/elasticsearch/reference/7.9/search-aggregations-bucket-terms-aggregation.html
//!
//! Returns JsonB that is a Terms ES Query

use crate::elasticsearch::aggregates::builders::make_children_map;
use crate::elasticsearch::aggregates::terms::pg_catalog::TermsOrderBy;
use pgx::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn terms_agg(
    aggregate_name: &str,
    field: &str,
    size_limit: i32,
    order_by: TermsOrderBy,
    children: Option<default!(Vec<JsonB>, NULL)>,
) -> JsonB {
    let order = match order_by {
        TermsOrderBy::count => json! {{ "_count": "asc" }},
        TermsOrderBy::term | TermsOrderBy::key => json! {{ "_key": "asc" }},
        TermsOrderBy::reverse_count => json! {{ "_count": "desc" }},
        TermsOrderBy::reverse_term | TermsOrderBy::reverse_key => {
            json! {{ "_key": "desc" }}
        }
    };

    JsonB(json! {
        {
            aggregate_name: {
                "terms": {
                    "field": field,
                    "shard_size": std::i32::MAX,
                    "size": size_limit,
                    "order": order
                },
                "aggs": make_children_map(children)
            }
        }
    })
}
