use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;

#[pg_extern(immutable, parallel_safe)]
fn arbitrary_agg(index: PgRelation, query: ZDBQuery, json: JsonB) -> JsonB {
    let elasticsearch = Elasticsearch::new(&index);

    let request = elasticsearch.arbitrary_aggregate::<serde_json::Value>(
        None,
        false,
        query.prepare(&index, None).0,
        json.0,
    );

    JsonB(
        request
            .execute()
            .expect("failed to execute aggregate search"),
    )
}
