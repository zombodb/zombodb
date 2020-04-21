use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use serde_json::*;

pub struct ElasticsearchUpdateSettingsRequest {
    elasticsearch: Elasticsearch,
    old_alias: Option<String>,
    new_alias: String,
}

impl ElasticsearchUpdateSettingsRequest {
    pub fn new(elasticsearch: &Elasticsearch, old_alias: Option<&str>) -> Self {
        ElasticsearchUpdateSettingsRequest {
            elasticsearch: elasticsearch.clone(),
            old_alias: if let Some(old_alias) = old_alias {
                Some(old_alias.to_owned())
            } else {
                None
            },
            new_alias: elasticsearch.options.alias().to_owned(),
        }
    }

    pub fn execute(self) -> std::result::Result<(), ElasticsearchError> {
        match Elasticsearch::execute_request(
            reqwest::Client::new()
                .put(&format!("{}/_settings", self.elasticsearch.base_url()))
                .header("content-type", "application/json")
                .body(
                    serde_json::to_string(&json! {
                        {
                            "index": {
                                "refresh_interval": self.elasticsearch.options.refresh_interval().as_str(),
                                "number_of_replicas": self.elasticsearch.options.replicas(),
                                "translog.durability": self.elasticsearch.options.translog_durability(),
                            }
                        }
                    })
                    .unwrap(),
                ),
            |_, _| Ok(()),
        ) {
            Ok(_) => {
                if let Some(old_alias) = self.old_alias {
                    // also update the alias
                    Elasticsearch::execute_request(
                        reqwest::Client::new()
                            .post(&format!("{}_aliases", self.elasticsearch.url()))
                            .header("content-type", "application/json")
                            .body(
                                serde_json::to_string(&json! {
                                {
                                   "actions": [      
                                        {"remove": { "index": self.elasticsearch.index_name(), "alias": old_alias } },      
                                        {"add": { "index": self.elasticsearch.index_name(), "alias": self.new_alias } }   
                                    ]
                                }
                            }).unwrap()),
                        |_, _| Ok(()),
                    )
                } else {
                    Ok(())
                }
            }
            Err(e) => Err(e),
        }
    }
}
