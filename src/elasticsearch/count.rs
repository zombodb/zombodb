use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::zdbquery::mvcc::apply_visibility_clause;
use crate::zdbquery::ZDBPreparedQuery;
use serde::*;
use serde_json::*;

pub struct ElasticsearchCountRequest {
    elasticsearch: Elasticsearch,
    query: ZDBPreparedQuery,
    raw: bool,
}

impl ElasticsearchCountRequest {
    pub fn new(elasticsearch: &Elasticsearch, query: ZDBPreparedQuery, raw: bool) -> Self {
        ElasticsearchCountRequest {
            elasticsearch: elasticsearch.clone(),
            query,
            raw,
        }
    }

    pub fn execute(self) -> std::result::Result<u64, ElasticsearchError> {
        let body = if self.raw {
            json! {
                {
                    "query": self.query.query_dsl()
                }
            }
        } else {
            json! {
                {
                    "query": apply_visibility_clause(&self.elasticsearch, self.query, false)
                }
            }
        };

        let mut url = self.elasticsearch.alias_url();
        url.push_str("/_count");
        Elasticsearch::execute_request(
            Elasticsearch::client()
                .post(&url)
                .header("content-type", "application/json")
                .body(serde_json::to_string(&body).unwrap()),
            |_, body| {
                #[derive(Deserialize)]
                struct Count {
                    count: u64,
                }

                let count: Count =
                    serde_json::from_str(&body).expect("failed to deserialize count response");
                Ok(count.count)
            },
        )
    }
}
