use crate::elasticsearch::search::SearchResponseIntoIter;
use crate::elasticsearch::Elasticsearch;
use crate::executor_manager::get_executor_manager;
use crate::zdbquery::ZDBQuery;
use pgx::*;

struct ZDBScanState {
    index_oid: pg_sys::Oid,
    iterator: *mut SearchResponseIntoIter,
}

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let mut scandesc: PgBox<pg_sys::IndexScanDescData> = unsafe {
        PgBox::from_pg(pg_sys::RelationGetIndexScan(
            index_relation,
            nkeys,
            norderbys,
        ))
    };
    let state = ZDBScanState {
        index_oid: unsafe { (*index_relation).rd_id },
        iterator: std::ptr::null_mut(),
    };

    scandesc.opaque =
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(state) as void_mut_ptr;

    scandesc.into_pg()
}

#[pg_guard]
pub extern "C" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: ::std::os::raw::c_int,
    _orderbys: pg_sys::ScanKey,
    _norderbys: ::std::os::raw::c_int,
) {
    if nkeys == 0 {
        panic!("No ScanKeys provided");
    }
    let scan: PgBox<pg_sys::IndexScanDescData> = unsafe { PgBox::from_pg(scan) };
    let indexrel = unsafe { PgRelation::from_pg(scan.indexRelation) };

    let mut state =
        unsafe { (scan.opaque as *mut ZDBScanState).as_mut() }.expect("no scandesc state");
    let nkeys = nkeys as usize;
    let keys = unsafe { std::slice::from_raw_parts(keys as *const pg_sys::ScanKeyData, nkeys) };
    let mut query = unsafe { ZDBQuery::from_datum(keys[0].sk_argument, false).unwrap() };

    // AND multiple keys together as a "bool": {"must":[....]} query
    for key in keys[1..nkeys].iter() {
        query = crate::query_dsl::bool::dsl::binary_and(query, unsafe {
            ZDBQuery::from_datum(key.sk_argument, false).unwrap()
        });
    }

    let elasticsearch = Elasticsearch::new(&indexrel);

    let response = elasticsearch
        .open_search(query.prepare(&indexrel, None).0)
        .execute()
        .unwrap_or_else(|e| panic!("{}", e));

    state.iterator =
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(response.into_iter());
}

#[pg_guard]
pub extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection,
) -> bool {
    let mut scan: PgBox<pg_sys::IndexScanDescData> = unsafe { PgBox::from_pg(scan) };
    let state = unsafe { (scan.opaque as *mut ZDBScanState).as_mut() }.expect("no scandesc state");

    // no need to recheck the returned tuples as ZomboDB indices are not lossy
    scan.xs_recheck = false;

    let iter = unsafe { state.iterator.as_mut() }.expect("no iterator in state");
    match iter.next() {
        Some((score, ctid, _, highlights)) => {
            #[cfg(any(feature = "pg10", feature = "pg11"))]
            let tid = &mut scan.xs_ctup.t_self;
            #[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14"))]
            let tid = &mut scan.xs_heaptid;

            u64_to_item_pointer(ctid, tid);
            if unsafe { !item_pointer_is_valid(tid) } {
                panic!("invalid item pointer: {:?}", item_pointer_get_both(*tid));
            }

            // TODO:  the score/highlight values we stash away here relates to the index ctid, not
            //        the heap ctid.  These could be different in the case of HOT-updated tuples
            //        it's not clear how we can efficiently resolve the HOT chain here.
            //        Likely side-effects of this will be that `zdb.score(ctid)` and `zdb.highlight(ctid...`
            //        will end up returning NULL on the SQL-side of things.
            let (_, qstate) = get_executor_manager().peek_query_state().unwrap();
            qstate.add_score(state.index_oid, ctid, score);
            qstate.add_highlight(state.index_oid, ctid, highlights);

            true
        }
        None => false,
    }
}

#[pg_guard]
pub extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {
    // nothing to do here
}

#[pg_guard]
pub extern "C" fn ambitmapscan(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64 {
    let scan = unsafe { PgBox::from_pg(scan) };
    let index_relation = unsafe { PgRelation::from_pg(scan.indexRelation) };
    let state = unsafe { (scan.opaque as *mut ZDBScanState).as_mut() }.expect("no scandesc state");
    let (_query, qstate) = get_executor_manager().peek_query_state().unwrap();

    let mut cnt = 0i64;
    let itr = unsafe { state.iterator.as_mut() }.expect("no iterator in state");
    for (score, ctid_u64, _, highlights) in itr {
        let mut tid = pg_sys::ItemPointerData::default();
        u64_to_item_pointer(ctid_u64, &mut tid);

        unsafe {
            pg_sys::tbm_add_tuples(tbm, &mut tid, 1, false);
        }

        qstate.add_score(index_relation.oid(), ctid_u64, score);
        qstate.add_highlight(index_relation.oid(), ctid_u64, highlights);
        cnt += 1;
    }

    cnt
}
