#![allow(dead_code)]

mod bulk;
mod create_index;

use crate::access_method::options::ZDBIndexOptions;
pub use bulk::*;
pub use create_index::*;
use pgx::{pg_sys, PgBox};
use serde_json::Value;

pub struct Elasticsearch<'a> {
    pub(crate) heaprel: &'a PgBox<pg_sys::RelationData>,
    pub(crate) indexrel: &'a PgBox<pg_sys::RelationData>,
    pub(crate) options: PgBox<ZDBIndexOptions>,
}

#[derive(Debug, Clone)]
pub enum ElasticsearchError {
    CreationError((reqwest::StatusCode, String)),
}

impl<'a> Elasticsearch<'a> {
    pub fn new(
        heaprel: &'a PgBox<pg_sys::RelationData>,
        indexrel: &'a PgBox<pg_sys::RelationData>,
    ) -> Self {
        Elasticsearch {
            heaprel,
            indexrel,
            options: unsafe { ZDBIndexOptions::from(indexrel) },
        }
    }

    pub fn create_index(&'a self, mapping: Value) -> ElasticsearchCreateIndexRequest<'a> {
        ElasticsearchCreateIndexRequest::new(self, mapping)
    }

    pub fn start_bulk(self) -> ElasticsearchBulkRequest<'a> {
        ElasticsearchBulkRequest::new(self, 10_000, num_cpus::get())
    }
}
