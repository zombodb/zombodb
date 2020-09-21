use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchRefreshIndexRequest(Elasticsearch);

impl ElasticsearchRefreshIndexRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchRefreshIndexRequest(elasticsearch.clone())
    }

    pub fn execute(&self) -> Result<(), ElasticsearchError> {
        Elasticsearch::execute_request(
            Elasticsearch::client().post(&format!("{}/_refresh", self.0.base_url())),
            |_, _| Ok(()),
        )
    }
}
