#![allow(dead_code)]

mod bulk;
mod create_index;
mod delete_index;

use crate::access_method::options::ZDBIndexOptions;
use crate::elasticsearch::delete_index::ElasticsearchDeleteIndexRequest;
pub use bulk::*;
pub use create_index::*;
use pgx::{pg_sys, PgBox};
use reqwest::RequestBuilder;
use serde_json::Value;
use std::io::Read;

pub struct Elasticsearch<'a> {
    pub(crate) heaprel: &'a PgBox<pg_sys::RelationData>,
    pub(crate) indexrel: &'a PgBox<pg_sys::RelationData>,
    pub(crate) options: PgBox<ZDBIndexOptions>,
}

#[derive(Debug)]
pub struct ElasticsearchError(Option<reqwest::StatusCode>, String);

impl ElasticsearchError {
    pub fn status(&self) -> Option<reqwest::StatusCode> {
        self.0
    }

    pub fn message(&self) -> &str {
        &self.1
    }
}

impl<'a> Elasticsearch<'a> {
    pub fn new(
        heaprel: &'a PgBox<pg_sys::RelationData>,
        indexrel: &'a PgBox<pg_sys::RelationData>,
    ) -> Self {
        Elasticsearch {
            heaprel,
            indexrel,
            options: ZDBIndexOptions::from(indexrel),
        }
    }

    pub fn create_index(&'a self, mapping: Value) -> ElasticsearchCreateIndexRequest<'a> {
        ElasticsearchCreateIndexRequest::new(self, mapping)
    }

    pub fn delete_index(&'a self) -> ElasticsearchDeleteIndexRequest {
        ElasticsearchDeleteIndexRequest::new(self.base_url())
    }

    pub fn start_bulk(self) -> ElasticsearchBulkRequest<'a> {
        ElasticsearchBulkRequest::new(self, 10_000, num_cpus::get())
    }

    pub fn base_url(&self) -> String {
        format!(
            "{}{}",
            self.options.url(),
            self.options.index_name(self.heaprel, self.indexrel)
        )
    }

    pub fn execute_request<F, R>(
        builder: RequestBuilder,
        response_parser: F,
    ) -> std::result::Result<R, ElasticsearchError>
    where
        F: FnOnce(reqwest::StatusCode, String) -> std::result::Result<R, ElasticsearchError>,
    {
        match builder.send() {
            // the request was processed by ES, but maybe not successfully
            Ok(mut response) => {
                let code = response.status();
                let mut body_string = String::new();
                response
                    .read_to_string(&mut body_string)
                    .expect("unable to convert HTTP response to a string");

                if code.as_u16() != 200 {
                    // it wasn't a valid response code
                    Err(ElasticsearchError(Some(code), body_string))
                } else {
                    response_parser(code, body_string)
                }
            }

            // the request didn't reach ES
            Err(e) => Err(ElasticsearchError(e.status(), e.to_string())),
        }
    }
}
