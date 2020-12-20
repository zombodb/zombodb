use crate::elasticsearch::{Elasticsearch, ElasticsearchError};

pub struct ElasticsearchCatRequest {
    elasticsearch: Elasticsearch,
    endpoint: String,
}

impl ElasticsearchCatRequest {
    pub fn new(elasticsearch: &Elasticsearch, endpoint: &str) -> Self {
        ElasticsearchCatRequest {
            elasticsearch: elasticsearch.clone(),
            endpoint: endpoint.to_owned(),
        }
    }

    pub fn execute(self) -> Result<String, ElasticsearchError> {
        let mut url = String::new();
        url.push_str(&self.elasticsearch.url());
        url.push_str("_cat/");
        url.push_str(&self.endpoint);
        url.push_str("?h=*&format=json&time=ms&bytes=b&size=k");

        Elasticsearch::execute_json_request(Elasticsearch::client().get(&url), None, |body| {
            let mut response = Vec::new();
            body.read_to_end(&mut response)
                .expect("failed to read response stream");
            Ok(String::from_utf8(response).expect("_cat request did not return valid UTF8"))
        })
    }
}
