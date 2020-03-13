use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::zdbquery::ZDBQuery;
use serde::*;
use serde_json::*;

pub struct ElasticsearchCountRequest {
    elasticsearch: Elasticsearch,
    query: ZDBQuery,
}

impl ElasticsearchCountRequest {
    pub fn new(elasticsearch: &Elasticsearch, query: ZDBQuery) -> Self {
        ElasticsearchCountRequest {
            elasticsearch: elasticsearch.clone(),
            query,
        }
    }

    pub fn execute(&self) -> std::result::Result<u64, ElasticsearchError> {
        Elasticsearch::execute_request(
            reqwest::Client::new()
                .post(&format!("{}/_count", self.elasticsearch.base_url()))
                .header("content-type", "application/json")
                .body(
                    serde_json::to_string(&json! {
                        {
                            "query": self.query.query_dsl()
                        }
                    })
                    .unwrap(),
                ),
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
