use crate::elasticsearch::{Elasticsearch, ElasticsearchBulkRequest};
use crate::mapping::{categorize_tupdesc, CategorizedAttribute};
use crate::utils::lookup_zdb_index_tupdesc;
pub use pgx::*;
use std::collections::HashMap;

static mut EXECUTOR_MANAGER: ExecutorManager = ExecutorManager::new();

pub fn get_executor_manager() -> &'static mut ExecutorManager {
    unsafe { &mut EXECUTOR_MANAGER }
}

pub struct BulkContext {
    pub bulk: ElasticsearchBulkRequest,
    pub attributes: Vec<CategorizedAttribute<'static>>,
    pub tupdesc: &'static PgTupleDesc<'static>,
}

pub struct ExecutorManager {
    depth: u32,
    tuple_descriptors: Option<HashMap<pg_sys::Oid, PgTupleDesc<'static>>>,
    bulk_requests: Option<HashMap<pg_sys::Oid, BulkContext>>,
}

impl ExecutorManager {
    pub const fn new() -> Self {
        ExecutorManager {
            depth: 0,
            tuple_descriptors: None,
            bulk_requests: None,
        }
    }

    pub fn push(&mut self) {
        self.depth += 1;
    }

    pub fn pop(&mut self) {
        self.depth -= 1;
    }

    pub fn on_end(&mut self) {
        if self.depth == 0 {
            self.finalize_bulk_requests();
            self.cleanup()
        }
    }

    pub fn abort(&mut self) {
        self.terminate_bulk_requests();
        self.cleanup()
    }

    fn finalize_bulk_requests(&mut self) {
        match self.bulk_requests.take() {
            Some(bulk_requests) => {
                // finish any of the bulk requests we have going on
                for (_, bulk) in bulk_requests.into_iter() {
                    match bulk.bulk.finish() {
                        Ok(cnt) => info!("indexed {} tuples", cnt),
                        Err(e) => panic!(e),
                    }
                }
            }
            None => {}
        }
    }

    fn terminate_bulk_requests(&mut self) {
        // forcefully terminate any of the bulk requests we have going on
        match self.bulk_requests.take() {
            Some(bulk_requests) => {
                for (_, bulk) in bulk_requests.into_iter() {
                    bulk.bulk.terminate_now();
                }
            }
            None => {}
        }
    }

    fn cleanup(&mut self) {
        self.tuple_descriptors.take();
        self.bulk_requests.take();
        self.depth = 0;
    }

    pub fn checkout_bulk_context(
        &'static mut self,
        relid: pg_sys::Oid,
    ) -> &'static mut BulkContext {
        if self.bulk_requests.is_none() {
            self.bulk_requests.replace(HashMap::new());
            self.tuple_descriptors.replace(HashMap::new());
        }

        let tupdesc_map = self.tuple_descriptors.as_mut().unwrap();
        let tupdesc = tupdesc_map.entry(relid).or_insert_with(|| {
            let indexrel = unsafe { PgRelation::open(relid) };
            lookup_zdb_index_tupdesc(&indexrel)
        });

        let bulk_map = self.bulk_requests.as_mut().unwrap();
        bulk_map.entry(relid).or_insert_with(move || {
            let indexrel = unsafe { PgRelation::open(relid) };
            let elasticsearch = Elasticsearch::new(&indexrel);
            let attributes = categorize_tupdesc(tupdesc, None);

            BulkContext {
                bulk: elasticsearch.start_bulk_with_refresh(),
                attributes,
                tupdesc,
            }
        })
    }
}
