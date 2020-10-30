use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde::*;
use serde_json::*;

#[pg_extern(immutable, parallel_safe)]
fn top_hits_with_id(
    index: PgRelation,
    fields: Array<&str>,
    query: ZDBQuery,
    size_limit: i64,
) -> impl std::iter::Iterator<Item = (name!(id, String), name!(score, f64), name!(source, Json))> {
    #[derive(Deserialize, Serialize)]
    struct TopHitsWithIdAggData {
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

    let request = elasticsearch.aggregate::<TopHitsWithIdAggData>(
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

    result
        .hits
        .hits
        .into_iter()
        .map(|entry| (entry._id, entry._score, Json(entry._source)))
}
