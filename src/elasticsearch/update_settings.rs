use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use serde_json::*;

pub struct ElasticsearchUpdateSettingsRequest(Elasticsearch);

impl ElasticsearchUpdateSettingsRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchUpdateSettingsRequest(elasticsearch.clone())
    }

    pub fn execute(self) -> std::result::Result<(), ElasticsearchError> {
        Elasticsearch::execute_request(
            Elasticsearch::client()
                .put(&format!("{}/_settings", self.0.base_url()))
                .header("content-type", "application/json")
                .body(
                    serde_json::to_string(&json! {
                        {
                            "index": {
                                "max_result_window": self.0.options.max_result_window(),
                                "refresh_interval": self.0.options.refresh_interval().as_str(),
                                "number_of_replicas": self.0.options.replicas(),
                                "translog.durability": self.0.options.translog_durability(),
                            }
                        }
                    })
                    .unwrap(),
                ),
            |_, _| Ok(()),
        )
    }
}
