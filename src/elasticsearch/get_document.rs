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

    pub fn execute(self) -> Result<Option<T>, ElasticsearchError> {
        let result = Elasticsearch::execute_json_request(
            Elasticsearch::client().get(&format!(
                "{}/_doc/{}?realtime={}",
                self.elasticsearch.base_url(),
                self.id,
                if self.realtime { "true" } else { "false" }
            )),
            None,
            |body| {
                //
                let value =
                    serde_json::from_reader(body).expect("failed to parse document into a value");
                Ok(serde_json::from_value::<T>(value).expect("failed to deserialize document"))
            },
        );

        match result {
            // a 404 is okay
            Err(e) if e.is_404() => Ok(None),
            // other errors are not
            Err(e) => Err(e),
            // it worked
            Ok(result) => Ok(Some(result)),
        }
    }
}
