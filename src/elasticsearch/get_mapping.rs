use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchGetMappingRequest(Elasticsearch);

impl ElasticsearchGetMappingRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchGetMappingRequest(elasticsearch.clone())
    }

    pub fn execute(&self) -> Result<serde_json::Value, ElasticsearchError> {
        Elasticsearch::execute_request(
            Elasticsearch::client().get(&format!("{}/_mapping", self.0.base_url())),
            |status, body| match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(value) => Ok(value),
                Err(e) => Err(ElasticsearchError(Some(status), e.to_string())),
            },
        )
    }
}
