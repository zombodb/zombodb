use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchDeleteIndexRequest(String);

impl ElasticsearchDeleteIndexRequest {
    pub fn new(url: String) -> Self {
        ElasticsearchDeleteIndexRequest(url)
    }

    pub fn execute(&self) -> Result<(), ElasticsearchError> {
        if let Err(e) =
            Elasticsearch::execute_request(reqwest::Client::new().delete(&self.0), |_, _| Ok(()))
        {
            if let Some(status) = e.status() {
                if status.as_u16() == 404 {
                    // 404 NOT FOUND is okay for us
                    return Ok(());
                }
            }

            // any other error, however, is worthy of reporting back to the caller
            return Err(e);
        }
        return Ok(());
    }
}
