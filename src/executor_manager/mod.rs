use crate::access_method::options::ZDBIndexOptions;
use crate::elasticsearch::{Elasticsearch, ElasticsearchBulkRequest};
use crate::mapping::{categorize_tupdesc, CategorizedAttribute};
use crate::utils::{find_zdb_index, lookup_all_zdb_index_oids, lookup_zdb_index_tupdesc};
use pgrx::itemptr::{item_pointer_get_both, u64_to_item_pointer_parts};
use pgrx::pg_sys::Oid;
use pgrx::*;
use std::collections::{HashMap, HashSet};

pub mod alter;
pub mod drop;
pub mod hooks;

static mut EXECUTOR_MANAGER: ExecutorManager = ExecutorManager::new();

pub fn get_executor_manager() -> &'static mut ExecutorManager {
    unsafe { &mut EXECUTOR_MANAGER }
}

pub struct BulkContext {
    pub elasticsearch: Elasticsearch,
    pub es_bulk_request: ElasticsearchBulkRequest,
    pub attributes: Vec<CategorizedAttribute>,
    pub natts: usize,
    pub dropped: Vec<bool>,
    pub is_shadow: bool,
    pub tupdesc: PgTupleDesc<'static>,
}

pub struct QueryState {
    scores: HashMap<(pg_sys::Oid, (pg_sys::BlockNumber, pg_sys::OffsetNumber)), f64>,
    highlights: HashMap<
        (pg_sys::Oid, (pg_sys::BlockNumber, pg_sys::OffsetNumber)),
        HashMap<String, Vec<String>>,
    >,
    zdb_index_lookup: HashMap<pg_sys::Oid, pg_sys::Oid>,
}

impl Default for QueryState {
    fn default() -> Self {
        QueryState {
            scores: HashMap::new(),
            highlights: HashMap::new(),
            zdb_index_lookup: Default::default(),
        }
    }
}

impl QueryState {
    #[inline]
    pub fn add_score(&mut self, heap_oid: pg_sys::Oid, ctid64: u64, score: f64) {
        if score > 0.0f64 {
            let key = (heap_oid, u64_to_item_pointer_parts(ctid64));
            let scores = &mut self.scores;

            // scores are additive for any given key,
            // so we start with zero and simply add this score to it
            *scores.entry(key).or_insert(0.0f64) += score;
        }
    }

    pub fn get_score(&self, heap_oid: pg_sys::Oid, ctid: pg_sys::ItemPointerData) -> f64 {
        *self
            .scores
            .get(&(heap_oid, item_pointer_get_both(ctid)))
            .unwrap_or(&0.0f64)
    }

    #[inline]
    pub fn add_highlight(
        &mut self,
        heap_oid: pg_sys::Oid,
        ctid64: u64,
        highlight: Option<HashMap<String, Vec<String>>>,
    ) {
        if let Some(highlight) = highlight {
            let key = (heap_oid, u64_to_item_pointer_parts(ctid64));
            let highlights = &mut self.highlights;

            match highlights.get_mut(&key) {
                Some(per_field) => {
                    for (k, mut v) in highlight.into_iter() {
                        let mut existing = per_field.get_mut(&k);

                        match existing.as_mut() {
                            Some(existing) => {
                                existing.append(&mut v);
                            }

                            None => {
                                per_field.insert(k, v);
                            }
                        }
                    }
                }
                None => {
                    highlights.insert(key, highlight);
                }
            }
        }
    }

    pub fn get_highlights(
        &self,
        heap_oid: pg_sys::Oid,
        ctid: pg_sys::ItemPointerData,
    ) -> Option<&HashMap<String, Vec<String>>> {
        self.highlights
            .get(&(heap_oid, item_pointer_get_both(ctid)))
    }

    pub fn get_highlight(
        &self,
        heap_oid: pg_sys::Oid,
        ctid: pg_sys::ItemPointerData,
        field: &str,
    ) -> Option<&Vec<String>> {
        if let Some(map) = self
            .highlights
            .get(&(heap_oid, item_pointer_get_both(ctid)))
        {
            map.get(field)
        } else {
            None
        }
    }

    pub fn lookup_index_for_first_field(
        &mut self,
        query_desc: *mut pg_sys::QueryDesc,
        fcinfo: pg_sys::FunctionCallInfo,
    ) -> Option<pg_sys::Oid> {
        let fcinfo = unsafe { PgBox::from_pg(fcinfo) };
        let flinfo = unsafe { PgBox::from_pg(fcinfo.flinfo) };
        let func_expr = unsafe { PgBox::from_pg(flinfo.fn_expr as *mut pg_sys::FuncExpr) };
        let arg_list = unsafe { PgList::<pg_sys::Node>::from_pg(func_expr.args) };
        let first_arg = arg_list
            .get_ptr(0)
            .expect("no arguments provided to zdb.score()");

        unsafe {
            if is_a(first_arg, pg_sys::NodeTag::T_Var) {
                // lookup the table from which the 'ctid' value comes, so we can get its oid
                let rtable = query_desc
                    .as_ref()
                    .unwrap()
                    .plannedstmt
                    .as_ref()
                    .unwrap()
                    .rtable;
                let var = PgBox::from_pg(first_arg as *mut pg_sys::Var);
                #[cfg(any(feature = "pg12"))]
                let rentry = pg_sys::rt_fetch(var.varnoold, rtable);
                #[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
                let rentry = pg_sys::rt_fetch(
                    if var.varnosyn != 0 {
                        var.varnosyn
                    } else {
                        var.varattnosyn as _
                    },
                    rtable,
                );
                let heap_oid = rentry.as_ref().unwrap().relid;

                if let Some(index_oid) = self.zdb_index_lookup.get(&heap_oid) {
                    return Some(*index_oid);
                }

                match find_zdb_index(&PgRelation::with_lock(
                    heap_oid,
                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                )) {
                    Ok((index, _)) => {
                        self.zdb_index_lookup.insert(heap_oid, index.oid());
                        Some(index.oid())
                    }
                    Err(_) => None,
                }
            } else if is_a(first_arg, pg_sys::NodeTag::T_FuncExpr) {
                let func_expr = PgBox::from_pg(first_arg as *mut pg_sys::FuncExpr);
                match lookup_all_zdb_index_oids() {
                    Some(oids) => {
                        let funcoid = func_expr.funcid;
                        for oid in oids {
                            let index = PgRelation::with_lock(
                                oid,
                                pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                            );
                            let exprs = PgList::<pg_sys::Expr>::from_pg(
                                pg_sys::RelationGetIndexExpressions(index.as_ptr()),
                            );

                            if let Some(expr) = exprs.get_ptr(0) {
                                if is_a(expr as *mut pg_sys::Node, pg_sys::NodeTag::T_FuncExpr) {
                                    let func_expr = PgBox::from_pg(expr as *mut pg_sys::FuncExpr);
                                    if func_expr.funcid == funcoid {
                                        return Some(index.oid());
                                    }
                                }
                            }
                        }

                        None
                    }
                    None => None,
                }
            } else {
                None
            }
        }
    }
}

pub struct ExecutorManager {
    bulk_requests: Option<HashMap<pg_sys::Oid, BulkContext>>,
    xids: Option<HashSet<pg_sys::TransactionId>>,
    query_stack: Option<Vec<(*mut pg_sys::QueryDesc, QueryState)>>,
    hooks_registered: bool,
}

impl ExecutorManager {
    pub const fn new() -> Self {
        ExecutorManager {
            bulk_requests: None,
            xids: None,
            query_stack: None,
            hooks_registered: false,
        }
    }

    pub fn push_query(&mut self, query_desc: &PgBox<pg_sys::QueryDesc>) {
        if self.query_stack.is_none() {
            self.query_stack.replace(Vec::new());
        }

        self.query_stack
            .as_mut()
            .unwrap()
            .push((query_desc.as_ptr(), QueryState::default()));
    }

    pub fn peek_query_state(&mut self) -> Option<&mut (*mut pg_sys::QueryDesc, QueryState)> {
        match self.query_stack.as_mut() {
            Some(stack) => {
                let len = stack.len();
                if len == 0 {
                    None
                } else {
                    stack.get_mut(len - 1)
                }
            }
            None => None,
        }
    }

    pub fn pop_query(&mut self) {
        self.query_stack.as_mut().unwrap().pop();
    }

    pub fn push_xid(&mut self, xid: pg_sys::TransactionId) {
        let xids = self.xids.as_mut().expect("no xids set in push");
        if !xids.contains(&xid) {
            xids.insert(xid);

            if let Some(bulk_requests) = self.bulk_requests.as_mut() {
                for (_, bulk) in bulk_requests.iter_mut() {
                    bulk.es_bulk_request
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

            for (key, bulk) in bulk_requests
                .into_iter()
                .filter(|(_, bulk)| !bulk.is_shadow)
            // shadow indexes don't change anything
            {
                let elasticsearch = bulk.elasticsearch;
                let attributes = bulk.attributes;
                let natts = bulk.natts;
                let dropped = bulk.dropped;
                let tupdesc = bulk.tupdesc;

                if let Err(e) = bulk.es_bulk_request.finish() {
                    panic!("{:?}", e)
                }

                let es_bulk_request = elasticsearch.start_bulk();
                replacement_requests.insert(
                    key,
                    BulkContext {
                        elasticsearch,
                        es_bulk_request,
                        attributes,
                        natts,
                        dropped,
                        is_shadow: false,
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
            for (_, mut bulk) in bulk_requests
                .into_iter()
                .filter(|(_, bulk)| !bulk.is_shadow)
            // shadow indexes don't do anything
            {
                for xid in self.xids.as_ref().unwrap().iter() {
                    bulk.es_bulk_request
                        .transaction_committed(*xid)
                        .expect("failed to mark transaction as committed");
                }

                if let Err(e) = bulk.es_bulk_request.finish() {
                    panic!("{:?}", e);
                }
            }
        }
    }

    fn terminate_bulk_requests(&mut self) {
        // forcefully terminate any of the bulk requests we have going on
        if let Some(bulk_requests) = self.bulk_requests.take() {
            for (_, bulk) in bulk_requests
                .into_iter()
                .filter(|(_, bulk)| !bulk.is_shadow)
            // shadow indexes don't do anything
            {
                bulk.es_bulk_request.terminate_now();
            }
        }
    }

    fn cleanup(&mut self) {
        self.bulk_requests.take();
        self.xids.take();
        self.query_stack.take();
        self.hooks_registered = false;
    }

    pub fn checkout_bulk_context(&'static mut self, indexrelid: Oid) -> &'static mut BulkContext {
        if self.bulk_requests.is_none() {
            self.bulk_requests.replace(HashMap::new());
            self.xids.replace(HashSet::new());
        }

        let bulk_map = self.bulk_requests.as_mut().unwrap();
        let xids = &self.xids;
        bulk_map.entry(indexrelid).or_insert_with(move || {
            let indexrel = unsafe { PgRelation::open(indexrelid) };
            let is_shadow = ZDBIndexOptions::is_shadow_index_fast(&indexrel);
            let elasticsearch = Elasticsearch::new(&indexrel);
            let tupdesc = lookup_zdb_index_tupdesc(&indexrel); // this is allocated in TopTransactionContext, which as far as we're concerned is effectively the `'static` lifetime
            let attributes = categorize_tupdesc(&tupdesc, &indexrel.heap_relation().unwrap(), None);

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
                        }
                    },
                );

                get_executor_manager().hooks_registered = true;
            }

            let mut es_bulk_request = elasticsearch.start_bulk();

            // only non-shadow indexes are written to
            if !is_shadow {
                // mark xids that are already known to be in progress as
                // also in progress for this new bulk context too
                if let Some(xids) = xids.as_ref() {
                    for xid in xids {
                        es_bulk_request
                            .transaction_in_progress(*xid)
                            .expect("Failed to mark transaction as in progress for new bulk");
                    }
                }
            }

            BulkContext {
                elasticsearch,
                es_bulk_request,
                attributes,
                natts: tupdesc.natts as _,
                dropped: (0..tupdesc.natts as usize)
                    .map(|i| tupdesc.get(i).unwrap().is_dropped())
                    .collect(),
                is_shadow,
                tupdesc,
            }
        })
    }
}
