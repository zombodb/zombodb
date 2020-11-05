use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;

#[pg_extern(immutable, parallel_safe)]
fn arbitrary_agg(index: PgRelation, query: ZDBQuery, json: Json) -> Json {
    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.aggregate::<serde_json::Value>(
        None,
        false,
        query.prepare(&index, None).0,
        json.0,
    );

    Json(
        request
            .execute()
            .expect("failed to execute aggregate search"),
    )
}
