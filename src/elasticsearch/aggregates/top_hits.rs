use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgrx::itemptr::u64_to_item_pointer;
use pgrx::prelude::*;
use pgrx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
pub fn top_hits(
    index: PgRelation,
    fields: Vec<String>,
    query: ZDBQuery,
    size_limit: i64,
) -> TableIterator<
    'static,
    (
        name!(id, pg_sys::ItemPointerData),
        name!(score, f64),
        name!(source, Json),
    ),
> {
    #[derive(Deserialize, Serialize)]
    struct TopHitsAggData {
        hits: Hits,
    }

    #[derive(Deserialize, Serialize)]
    struct Hits {
        hits: Vec<HitsEntry>,
    }

    #[derive(Deserialize, Serialize)]
    struct HitsEntry {
        _id: String,
        _score: f64,
        _source: serde_json::Value,
    }

    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<TopHitsAggData>(
        None,
        false,
        query.prepare(&index, None).0,
        json! {
            {
                "top_hits": {
                    "_source": {
                        "includes": fields
                    },
                    "size": size_limit
                }
            }
        },
    );

    let result = request
        .execute()
        .expect("failed to execute aggregate search");

    let mut result_hits = Vec::new();

    for hits in result.hits.hits {
        let mut tid = pg_sys::ItemPointerData::default();
        let id = hits._id.parse::<u64>().unwrap();
        u64_to_item_pointer(id, &mut tid);
        result_hits.push((tid, hits._score, Json(hits._source)))
    }
    TableIterator::new(result_hits)
}
