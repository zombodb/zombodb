use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchGetSettingsRequest(Elasticsearch);

impl ElasticsearchGetSettingsRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchGetSettingsRequest(elasticsearch.clone())
    }

    pub fn execute(&self) -> Result<serde_json::Value, ElasticsearchError> {
        Elasticsearch::execute_request(
            Elasticsearch::client().get(&format!("{}/_settings", self.0.base_url())),
            |status, body| match serde_json::from_str::<serde_json::Value>(&body) {
                Ok(value) => Ok(value),
                Err(e) => Err(ElasticsearchError(Some(status), e.to_string())),
            },
        )
    }
}
