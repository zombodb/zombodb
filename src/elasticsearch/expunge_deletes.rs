use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchExpungeDeletesRequest(Elasticsearch);

impl ElasticsearchExpungeDeletesRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchExpungeDeletesRequest(elasticsearch.clone())
    }

    pub fn execute(self) -> Result<(), ElasticsearchError> {
        Elasticsearch::execute_json_request(
            Elasticsearch::client().post(&format!(
                "{}/_forcemerge?only_expunge_deletes=true&flush=false",
                self.0.base_url()
            )),
            None,
            |_| Ok(()),
        )
    }
}
