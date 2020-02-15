use crate::elasticsearch::search::SearchResponseIntoIter;
use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;

struct ZDBScanState {
    ntuples: u64,
    zdbregtype: pg_sys::Oid,
    iterator: SearchResponseIntoIter,
}

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let mut scandesc: PgBox<pg_sys::IndexScanDescData> =
        PgBox::from_pg(unsafe { pg_sys::RelationGetIndexScan(index_relation, nkeys, norderbys) });

    info!("ambeginscan");

    let mut state = PgBox::<ZDBScanState>::alloc0();

    state.ntuples = 0;
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
    info!("amrescan: nkeys={}", nkeys);
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
    let heaprel = PgBox::from_pg(scan.heapRelation);
    let indexrel = PgBox::from_pg(scan.indexRelation);
    let es = Elasticsearch::new(&heaprel, &indexrel);
    info!("query={}", serde_json::to_string(&query).unwrap());

    let response = es
        .open_search(query)
        .execute()
        .expect("failed to execute ES query");

    state.iterator = response.into_iter();
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

    match state.iterator.next() {
        Some((_score, ctid)) => {
            u64_to_item_pointer(ctid, &mut scan.xs_ctup.t_self);

            state.ntuples += 1;
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
pub extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {
    info!("amendscan");
}
