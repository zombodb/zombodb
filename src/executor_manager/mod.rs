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
}

pub struct ExecutorManager {
    depth: u32,
    tuple_descriptors: Option<HashMap<pg_sys::Oid, PgTupleDesc>>,
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

        if self.depth == 0 {
            info!("self.depth == 0");
            self.cleanup()
        }
    }

    pub fn depth(&self) -> u32 {
        self.depth
    }

    pub fn abort(&mut self) {
        // TODO:  Terminate the bulk requests
        self.cleanup()
    }

    fn cleanup(&mut self) {
        self.tuple_descriptors.replace(HashMap::new());
        self.bulk_requests.replace(HashMap::new());
        self.depth = 0;
    }

    pub fn checkout_bulk_context(
        &'static mut self,
        indexrel: &PgRelation,
    ) -> &'static mut BulkContext {
        if self.bulk_requests.is_none() {
            self.bulk_requests.replace(HashMap::new());
            self.tuple_descriptors.replace(HashMap::new());
        }

        let relid = indexrel.oid();
        let tupdesc_map = self.tuple_descriptors.as_mut().unwrap();
        let tupdesc = tupdesc_map
            .entry(relid)
            .or_insert_with(|| lookup_zdb_index_tupdesc(&indexrel));

        let bulk_map = self.bulk_requests.as_mut().unwrap();
        bulk_map.entry(relid).or_insert_with(move || {
            let heap_relation = indexrel.get_heap_relation().expect("not an index");
            let elasticsearch = Elasticsearch::new(&heap_relation, indexrel);
            let attributes = categorize_tupdesc(tupdesc, None);

            info!("checking out new bulk context");
            BulkContext {
                bulk: elasticsearch.start_bulk(),
                attributes,
            }
        })
    }
}
