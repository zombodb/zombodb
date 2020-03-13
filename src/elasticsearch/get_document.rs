use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use std::marker::PhantomData;

pub struct ElasticsearchGetDocumentRequest<'a, T: serde::de::DeserializeOwned> {
    elasticsearch: Elasticsearch,
    id: &'a str,
    realtime: bool,
    _marker: PhantomData<T>,
}

impl<'a, T: serde::de::DeserializeOwned> ElasticsearchGetDocumentRequest<'a, T> {
    pub fn new(
        elasticsearch: &Elasticsearch,
        id: &'a str,
        realtime: bool,
    ) -> ElasticsearchGetDocumentRequest<'a, T> {
        ElasticsearchGetDocumentRequest::<T> {
            elasticsearch: elasticsearch.clone(),
            id,
            realtime,
            _marker: PhantomData,
        }
    }

    pub fn execute(&self) -> Result<Option<T>, ElasticsearchError> {
        let result = Elasticsearch::execute_request(
            reqwest::Client::new().get(&format!(
                "{}/_doc/{}?realtime={}",
                self.elasticsearch.base_url(),
                self.id,
                if self.realtime { "true" } else { "false" }
            )),
            |_, body| {
                //
                let value =
                    serde_json::from_str(&body).expect("failed to parse document into a value");
                Ok(serde_json::from_value::<T>(value).expect("failed to deserialize document"))
            },
        );

        match result {
            Ok(result) => Ok(Some(result)),
            Err(e) => {
                if e.status().is_some() && e.status().unwrap().as_u16() == 404 {
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }
}
