use pgrx::*;

use crate::elasticsearch::Elasticsearch;

#[pg_extern(immutable, parallel_safe)]
pub fn cat_request(index: PgRelation, endpoint: &str) -> JsonB {
    let es = Elasticsearch::new(&index);

    let result = serde_json::from_str::<serde_json::Value>(
        &es.cat(endpoint)
            .execute()
            .expect("failed to execute _cat request"),
    )
    .expect("failed to convert _cat response to json");
    JsonB(result)
}
