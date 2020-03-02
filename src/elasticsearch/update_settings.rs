use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use serde_json::*;

pub struct ElasticsearchUpdateSettingsRequest(Elasticsearch);

impl ElasticsearchUpdateSettingsRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchUpdateSettingsRequest(elasticsearch.clone())
    }

    pub fn execute(self) -> std::result::Result<(), ElasticsearchError> {
        Elasticsearch::execute_request(
            reqwest::Client::new()
                .put(&format!("{}/_settings", self.0.base_url()))
                .header("content-type", "application/json")
                .body(
                    serde_json::to_string(&json! {
                        {
                            "index": {
                                "refresh_interval": self.0.options.refresh_interval.as_str(),
                                "number_of_replicas": self.0.options.replicas
                            }
                        }
                    })
                    .unwrap(),
                ),
            |_, _| Ok(()),
        )
    }
}
