use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use serde_json::json;

pub struct ElasticsearchPutMappingRequest {
    elasticsearch: Elasticsearch,
    mapping: serde_json::Value,
}

impl ElasticsearchPutMappingRequest {
    pub fn new(elasticsearch: &Elasticsearch, mapping: serde_json::Value) -> Self {
        ElasticsearchPutMappingRequest {
            elasticsearch: elasticsearch.clone(),
            mapping,
        }
    }

    pub fn execute(self) -> std::result::Result<(), ElasticsearchError> {
        let body = json! {
            {
                "properties": self.mapping
            }
        };

        let mut url = self.elasticsearch.base_url();
        url.push_str("/_mapping");
        Elasticsearch::execute_json_request(Elasticsearch::client().post(&url), Some(body), |_| {
            Ok(())
        })
    }
}
