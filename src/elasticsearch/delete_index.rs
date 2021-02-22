use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchDeleteIndexRequest(Elasticsearch);

impl ElasticsearchDeleteIndexRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchDeleteIndexRequest(elasticsearch.clone())
    }

    pub fn execute(self) -> Result<(), ElasticsearchError> {
        match Elasticsearch::execute_json_request(
            Elasticsearch::client().delete(&self.0.base_url()),
            None,
            |_| Ok(()),
        ) {
            // 404 NOT FOUND is okay for us
            Err(e) if e.is_404() => Ok(()),

            // but other errors need to be reported back to the caller
            Err(e) => Err(e),

            // it worked as expected
            Ok(v) => Ok(v),
        }
    }
}
