use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchGetSettingsRequest(Elasticsearch);

impl ElasticsearchGetSettingsRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchGetSettingsRequest(elasticsearch.clone())
    }

    pub fn execute(self) -> Result<serde_json::Value, ElasticsearchError> {
        Elasticsearch::execute_json_request(
            Elasticsearch::client().get(&format!("{}/_settings", self.0.base_url())),
            None,
            |body| Ok(serde_json::from_reader(body).expect("failed to parse json response")),
        )
    }
}
