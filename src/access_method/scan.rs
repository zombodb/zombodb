use crate::elasticsearch::search::SearchResponseIntoIter;
use crate::elasticsearch::Elasticsearch;
use crate::executor_manager::get_executor_manager;
use crate::zdbquery::ZDBQuery;
use pgx::*;

struct ZDBScanState {
    zdbregtype: pg_sys::Oid,
    iterator: *mut SearchResponseIntoIter,
}

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let mut scandesc: PgBox<pg_sys::IndexScanDescData> =
        PgBox::from_pg(unsafe { pg_sys::RelationGetIndexScan(index_relation, nkeys, norderbys) });
    let state = ZDBScanState {
        zdbregtype: unsafe {
            direct_function_call::<pg_sys::Oid>(
                pg_sys::to_regtype,
                vec!["pg_catalog.zdbquery".into_datum()],
            )
            .expect("failed to lookup type oid for pg_catalog.zdbquery")
        },
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
    let scan: PgBox<pg_sys::IndexScanDescData> = PgBox::from_pg(scan);
    let mut state =
        unsafe { (scan.opaque as *mut ZDBScanState).as_mut() }.expect("no scandesc state");
    let nkeys = nkeys as usize;
    let keys = unsafe { std::slice::from_raw_parts(keys as *const pg_sys::ScanKeyData, nkeys) };
    let mut query =
        unsafe { ZDBQuery::from_datum(keys[0].sk_argument, false, state.zdbregtype).unwrap() };

    // AND multiple keys together as a "bool": {"must":[....]} query
    for key in keys[1..nkeys].iter() {
        query = crate::query_dsl::bool::dsl::binary_and(query, unsafe {
            ZDBQuery::from_datum(key.sk_argument, false, state.zdbregtype).unwrap()
        });
    }

    let indexrel = unsafe { PgRelation::from_pg(scan.indexRelation) };
    let elasticsearch = Elasticsearch::new(&indexrel);

    let response = elasticsearch
        .open_search(query)
        .execute()
        .expect("failed to execute ES query");

    state.iterator =
        PgMemoryContexts::CurrentMemoryContext.leak_and_drop_on_delete(response.into_iter());
}

#[pg_guard]
pub extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection,
) -> bool {
    let mut scan: PgBox<pg_sys::IndexScanDescData> = PgBox::from_pg(scan);
    let heap_relation = unsafe { PgRelation::from_pg(scan.heapRelation) };
    let state = unsafe { (scan.opaque as *mut ZDBScanState).as_mut() }.expect("no scandesc state");

    // no need to recheck the returned tuples as ZomboDB indices are not lossy
    scan.xs_recheck = false;

    let iter = unsafe { state.iterator.as_mut() }.expect("no iterator in state");
    match iter.next() {
        Some((score, ctid, _)) => {
            #[cfg(any(feature = "pg10", feature = "pg11"))]
            let tid = &mut scan.xs_ctup.t_self;

            #[cfg(feature = "pg12")]
            let tid = &mut scan.xs_heaptid;

            u64_to_item_pointer(ctid, tid);

            let (_query, qstate) = get_executor_manager().peek_query_state().unwrap();
            qstate.add_score(heap_relation.oid(), ctid, score);

            if !item_pointer_is_valid(tid) {
                panic!("invalid item pointer: {:?}", item_pointer_get_both(*tid));
            }
            true
        }
        None => false,
    }
}

#[pg_guard]
pub extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {
    // nothing to do here
}
