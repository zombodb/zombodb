use crate::elasticsearch::search::SearchResponseIntoIter;
use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;

struct ZDBScanState {
    zdbregtype: pg_sys::Oid,
    iterator: *mut SearchResponseIntoIter,
    abort_receipt: Option<XactCallbackReceipt>,
}

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    info!("ambeginscan");
    let mut scandesc: PgBox<pg_sys::IndexScanDescData> =
        PgBox::from_pg(unsafe { pg_sys::RelationGetIndexScan(index_relation, nkeys, norderbys) });
    let mut state = PgBox::<ZDBScanState>::alloc0();

    state.zdbregtype = unsafe {
        direct_function_call::<pg_sys::Oid>(
            pg_sys::to_regtype,
            vec!["pg_catalog.zdbquery".into_datum()],
        )
        .expect("failed to lookup type oid for pg_catalog.zdbquery")
    };

    scandesc.opaque = state.into_pg() as void_mut_ptr;

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
    info!("amrescan");
    if nkeys == 0 {
        panic!("No ScanKeys provided");
    }
    let scan: PgBox<pg_sys::IndexScanDescData> = PgBox::from_pg(scan);
    let mut state = PgBox::from_pg(scan.opaque as *mut ZDBScanState);
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

    // TODO:  query elasticsearch
    let indexrel = PgRelation::from_pg(scan.indexRelation);
    let elasticsearch = Elasticsearch::new(&indexrel);

    let response = elasticsearch
        .open_search(query)
        .execute()
        .expect("failed to execute ES query");

    // we convert the response from above into a Boxed iterator
    // and then we leak it so we can hold onto a reference to it in case
    // the transaction aborts
    let iterator = Box::new(response.into_iter());
    state.iterator = Box::leak(iterator);

    // and if the transaction does abort, we'll re-box the iterator and then
    // immediately drop it, ensuring we don't leak anything across transactions
    let iter_ptr = state.iterator as void_mut_ptr;
    state.abort_receipt = Some(register_xact_callback(
        PgXactCallbackEvent::Abort,
        move || {
            // drop the iterator
            let iter = unsafe { Box::from_raw(iter_ptr as *mut SearchResponseIntoIter) };
            drop(iter);
        },
    ));
}

#[pg_guard]
pub extern "C" fn amgettuple(
    scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection,
) -> bool {
    let mut scan: PgBox<pg_sys::IndexScanDescData> = PgBox::from_pg(scan);
    let mut state = PgBox::from_pg(scan.opaque as *mut ZDBScanState);

    // no need to recheck the returned tuples as ZomboDB indices are not lossy
    scan.xs_recheck = false;

    let iter = unsafe { &mut *state.iterator };
    match iter.next() {
        Some((_score, ctid)) => {
            #[cfg(any(feature = "pg10", feature = "pg11"))]
            let tid = &mut scan.xs_ctup.t_self;

            #[cfg(feature = "pg12")]
            let tid = &mut scan.xs_heaptid;

            u64_to_item_pointer(ctid, tid);
            true
        }
        None => false,
    }
}

#[pg_guard]
pub extern "C" fn amgetbitmap(_scan: pg_sys::IndexScanDesc, _tbm: *mut pg_sys::TIDBitmap) -> i64 {
    info!("amgetbitmap");

    0
}

#[pg_guard]
pub extern "C" fn amendscan(scan: pg_sys::IndexScanDesc) {
    let scan: PgBox<pg_sys::IndexScanDescData> = PgBox::from_pg(scan);
    let mut state = PgBox::from_pg(scan.opaque as *mut ZDBScanState);

    // unregister the abort callback
    state
        .abort_receipt
        .take()
        .expect("no amscan abort receipt")
        .unregister_callback();

    // drop the iterator
    let iter = unsafe { Box::from_raw(state.iterator as *mut SearchResponseIntoIter) };
    drop(iter);
}
