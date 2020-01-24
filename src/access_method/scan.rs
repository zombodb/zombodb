use pgx::*;

#[pg_guard]
pub extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    let scandesc =
        PgBox::from_pg(unsafe { pg_sys::RelationGetIndexScan(index_relation, nkeys, norderbys) });

    scandesc.into_pg()
}

#[pg_guard]
pub extern "C" fn amrescan(
    _scan: pg_sys::IndexScanDesc,
    _keys: pg_sys::ScanKey,
    _nkeys: ::std::os::raw::c_int,
    _orderbys: pg_sys::ScanKey,
    _norderbys: ::std::os::raw::c_int,
) {
}

#[pg_guard]
pub extern "C" fn amgettuple(
    _scan: pg_sys::IndexScanDesc,
    _direction: pg_sys::ScanDirection,
) -> bool {
    false
}

#[pg_guard]
pub extern "C" fn amgetbitmap(_scan: pg_sys::IndexScanDesc, _tbm: *mut pg_sys::TIDBitmap) -> i64 {
    0
}

#[pg_guard]
pub extern "C" fn amendscan(_scan: pg_sys::IndexScanDesc) {}
