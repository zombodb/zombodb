use crate::elasticsearch::{Elasticsearch, ElasticsearchBulkRequest};
use crate::mapping::{categorize_tupdesc, CategorizedAttribute};
use crate::utils::lookup_zdb_index_tupdesc;
use pgx::*;
use std::collections::{HashMap, HashSet};

pub mod hooks;

static mut EXECUTOR_MANAGER: ExecutorManager = ExecutorManager::new();

pub fn get_executor_manager() -> &'static mut ExecutorManager {
    unsafe { &mut EXECUTOR_MANAGER }
}

pub struct BulkContext {
    pub elasticsearch: Elasticsearch,
    pub bulk: ElasticsearchBulkRequest,
    pub attributes: Vec<CategorizedAttribute<'static>>,
    pub tupdesc: &'static PgTupleDesc<'static>,
}

pub struct QueryState {
    scores: HashMap<(pg_sys::Oid, (pg_sys::BlockNumber, pg_sys::OffsetNumber)), f64>,
}

impl Default for QueryState {
    fn default() -> Self {
        QueryState {
            scores: HashMap::new(),
        }
    }
}

impl QueryState {
    pub fn add_score(&mut self, heap_oid: pg_sys::Oid, ctid64: u64, score: f64) {
        let key = (heap_oid, u64_to_item_pointer_parts(ctid64));
        let scores = &mut self.scores;

        // scores are additive for any given key,
        // so we start with zero and simply add this score to it
        *scores.entry(key).or_insert(0.0f64) += score;
    }

    pub fn get_score(&self, heap_oid: pg_sys::Oid, ctid: pg_sys::ItemPointerData) -> f64 {
        *self
            .scores
            .get(&(heap_oid, item_pointer_get_both(ctid)))
            .unwrap_or(&0.0f64)
    }
}

pub struct ExecutorManager {
    tuple_descriptors: Option<HashMap<pg_sys::Oid, PgTupleDesc<'static>>>,
    bulk_requests: Option<HashMap<pg_sys::Oid, BulkContext>>,
    xids: Option<HashSet<pg_sys::TransactionId>>,
    query_stack: Option<Vec<(*mut pg_sys::QueryDesc, QueryState)>>,
    hooks_registered: bool,
}

impl ExecutorManager {
    pub const fn new() -> Self {
        ExecutorManager {
            tuple_descriptors: None,
            bulk_requests: None,
            xids: None,
            query_stack: None,
            hooks_registered: false,
        }
    }

    pub fn push_query(&mut self, query_desc: &PgBox<pg_sys::QueryDesc>) {
        info!("push query");
        if self.query_stack.is_none() {
            self.query_stack.replace(Vec::new());
        }

        self.query_stack
            .as_mut()
            .unwrap()
            .push((query_desc.as_ptr(), QueryState::default()));
    }

    pub fn peek_query_state(&mut self) -> Option<&mut (*mut pg_sys::QueryDesc, QueryState)> {
        let stack = self.query_stack.as_mut().unwrap();
        let len = stack.len() - 1;
        stack.get_mut(len)
    }

    pub fn pop_query(&mut self) {
        info!("pop query");
        self.query_stack.as_mut().unwrap().pop();
    }

    pub fn push_xid(&mut self, xid: pg_sys::TransactionId) {
        let xids = self.xids.as_mut().expect("no xids set in push");
        if !xids.contains(&xid) {
            xids.insert(xid);

            if let Some(bulk_requests) = self.bulk_requests.as_mut() {
                for (_, bulk) in bulk_requests.iter_mut() {
                    info!("pushing xid={}", xid);
                    bulk.bulk
                        .transaction_in_progress(xid)
                        .expect("Failed to mark transaction as in progress for existing bulk");
                }
            }
        }
    }

    pub fn pop_xid(&mut self, xid: pg_sys::TransactionId) {
        self.xids
            .as_mut()
            .expect("xids set not initialized in pop")
            .remove(&xid);
    }

    pub fn used_xids(&self) -> Vec<u64> {
        if let Some(xids) = self.xids.as_ref() {
            xids.iter().map(|v| xid_to_64bit(*v)).collect::<Vec<u64>>()
        } else {
            Vec::new()
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

    /// for use by code that needs to execute a search within a transaction
    pub fn wait_for_completion(&mut self) {
        if let Some(bulk_requests) = self.bulk_requests.take() {
            let mut replacement_requests = HashMap::with_capacity(bulk_requests.capacity());

            for (key, bulk) in bulk_requests.into_iter() {
                let elasticsearch = bulk.elasticsearch;
                let attributes = bulk.attributes;
                let tupdesc = bulk.tupdesc;

                match bulk.bulk.finish() {
                    Ok((ntuples, nrequests)) => {
                        info!("indexed {} tuples in {} requests", ntuples, nrequests)
                    }
                    Err(e) => panic!(e),
                }

                let bulk = elasticsearch.start_bulk();
                replacement_requests.insert(
                    key,
                    BulkContext {
                        elasticsearch,
                        bulk,
                        attributes,
                        tupdesc,
                    },
                );
            }

            self.bulk_requests.replace(replacement_requests);
        }
    }

    fn finalize_bulk_requests(&mut self) {
        if let Some(bulk_requests) = self.bulk_requests.take() {
            // finish any of the bulk requests we have going on
            for (_, mut bulk) in bulk_requests.into_iter() {
                for xid in self.xids.as_ref().unwrap().iter() {
                    bulk.bulk
                        .transaction_committed(*xid)
                        .expect("failed to mark transaction as committed");
                }

                match bulk.bulk.finish() {
                    Ok((ntuples, nrequests)) => {
                        info!("indexed {} tuples in {} requests", ntuples, nrequests)
                    }
                    Err(e) => panic!(e),
                }
            }
        }
    }

    fn terminate_bulk_requests(&mut self) {
        // forcefully terminate any of the bulk requests we have going on
        if let Some(bulk_requests) = self.bulk_requests.take() {
            for (_, bulk) in bulk_requests.into_iter() {
                bulk.bulk.terminate_now();
            }
        }
    }

    fn cleanup(&mut self) {
        self.tuple_descriptors.take();
        self.bulk_requests.take();
        self.xids.take();
        self.query_stack.take();
        self.hooks_registered = false;
    }

    pub fn checkout_bulk_context(
        &'static mut self,
        relid: pg_sys::Oid,
    ) -> &'static mut BulkContext {
        if self.bulk_requests.is_none() {
            self.bulk_requests.replace(HashMap::new());
            self.tuple_descriptors.replace(HashMap::new());
            self.xids.replace(HashSet::new());
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

            if !get_executor_manager().hooks_registered {
                // called when the top-level transaction commits
                register_xact_callback(PgXactCallbackEvent::PreCommit, || {
                    get_executor_manager().commit()
                });

                // called when the top-level transaction aborts
                register_xact_callback(PgXactCallbackEvent::Abort, || {
                    get_executor_manager().abort()
                });

                // called when a subtransaction aborts
                register_subxact_callback(
                    PgSubXactCallbackEvent::AbortSub,
                    |_my_sub_id, _parent_sub_id| {
                        let current_xid = unsafe { pg_sys::GetCurrentTransactionIdIfAny() };
                        if current_xid != pg_sys::InvalidTransactionId {
                            get_executor_manager().pop_xid(current_xid);
                            info!("got aborted subtransaction: {}", current_xid);
                        }
                    },
                );

                get_executor_manager().hooks_registered = true;
            }

            info!("creating bulk context for {}", relid);
            let bulk = elasticsearch.start_bulk();
            BulkContext {
                elasticsearch,
                bulk,
                attributes,
                tupdesc,
            }
        })
    }
}
