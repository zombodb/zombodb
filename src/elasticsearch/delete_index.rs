use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchDeleteIndexRequest(Elasticsearch);

impl ElasticsearchDeleteIndexRequest {
    pub fn new(elasticsearch: &Elasticsearch) -> Self {
        ElasticsearchDeleteIndexRequest(elasticsearch.clone())
    }

    pub fn execute(&self) -> Result<(), ElasticsearchError> {
        if let Err(e) = Elasticsearch::execute_request(
            Elasticsearch::client().delete(&self.0.base_url()),
            |_, _| Ok(()),
        ) {
            if let Some(status) = e.status() {
                if status.as_u16() == 404 {
                    // 404 NOT FOUND is okay for us
                    return Ok(());
                }
            }

            // any other error, however, is worthy of reporting back to the caller
            return Err(e);
        }
        Ok(())
    }
}
