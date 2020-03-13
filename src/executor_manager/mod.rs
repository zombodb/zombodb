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
    tuple_descriptors: Option<HashMap<pg_sys::Oid, PgTupleDesc<'static>>>,
    bulk_requests: Option<HashMap<pg_sys::Oid, BulkContext>>,
}

impl ExecutorManager {
    pub const fn new() -> Self {
        ExecutorManager {
            tuple_descriptors: None,
            bulk_requests: None,
        }
    }

    pub fn abort(&mut self) {
        self.terminate_bulk_requests();
        self.cleanup();
    }

    pub fn commit(&mut self) {
        self.finalize_bulk_requests();
        self.cleanup();
    }

    fn finalize_bulk_requests(&mut self) {
        match self.bulk_requests.take() {
            Some(bulk_requests) => {
                // finish any of the bulk requests we have going on
                for (_, mut bulk) in bulk_requests.into_iter() {
                    bulk.bulk
                        .transaction_committed(unsafe { pg_sys::GetCurrentTransactionId() })
                        .expect("failed to mark transaction as committed");
                    match bulk.bulk.finish() {
                        Ok((ntuples, nrequests)) => {
                            info!("indexed {} tuples in {} requests", ntuples, nrequests)
                        }
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
            let attributes = categorize_tupdesc(tupdesc, &indexrel.heap_relation().unwrap(), None);

            let mut bulk = BulkContext {
                bulk: elasticsearch.start_bulk(),
                attributes,
                tupdesc,
            };

            bulk.bulk
                .transaction_in_progress(unsafe { pg_sys::GetCurrentTransactionId() })
                .expect("failed to mark transaction as in progress");

            register_xact_callback(PgXactCallbackEvent::PreCommit, || {
                get_executor_manager().commit()
            });
            register_xact_callback(PgXactCallbackEvent::Abort, || {
                get_executor_manager().abort()
            });

            bulk
        })
    }
}
