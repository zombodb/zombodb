use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::zdbquery::{ZDBPreparedQuery, ZDBQuery};
use pgrx::*;
use serde_json::*;

pub struct ElasticsearchProfileQueryRequest {
    elasticsearch: Elasticsearch,
    query: ZDBPreparedQuery,
}

impl ElasticsearchProfileQueryRequest {
    pub fn new(elasticsearch: &Elasticsearch, query: ZDBPreparedQuery) -> Self {
        ElasticsearchProfileQueryRequest {
            elasticsearch: elasticsearch.clone(),
            query,
        }
    }

    pub fn execute(self) -> std::result::Result<serde_json::Value, ElasticsearchError> {
        let body = json! {
            {
                "profile": true,
                "query": self.query.query_dsl()
            }
        };

        let mut url = String::new();
        url.push_str(&self.elasticsearch.base_url());
        url.push_str("/_search");
        url.push_str("?size=0");
        url.push_str("&filter_path=profile");
        Elasticsearch::execute_json_request(
            Elasticsearch::client().post(&url),
            Some(body),
            |body| Ok(serde_json::from_reader(body).expect("failed to parse response json")),
        )
    }
}

#[pg_extern(immutable, parallel_safe)]
fn profile_query(index: PgRelation, query: ZDBQuery) -> JsonB {
    JsonB(
        Elasticsearch::new(&index)
            .profile_query(query.prepare(&index, None).0)
            .execute()
            .expect("failed to execute profile query request"),
    )
}
