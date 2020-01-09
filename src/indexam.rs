use pgx::*;

#[pg_extern]
fn amhandler(fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine = PgNodeFactory::makeIndexAmRoutine();

    amroutine.amstrategies = 4;
    amroutine.amsupport = 0;
    amroutine.amcanmulticol = true;
    amroutine.amsearcharray = true;

    amroutine.amkeytype = pg_sys::InvalidOid;

    amroutine.amvalidate = Some(amvalidate);
    amroutine.ambuild = Some(ambuild);
    amroutine.ambuildempty = Some(ambuildempty);
    amroutine.aminsert = Some(aminsert);
    amroutine.ambulkdelete = Some(ambulkdelete);
    amroutine.amvacuumcleanup = Some(amvacuumcleanup);
    amroutine.amcostestimate = Some(amcostestimate);
    amroutine.amoptions = Some(amoptions);
    amroutine.ambeginscan = Some(ambeginscan);
    amroutine.amrescan = Some(amrescan);
    amroutine.amgettuple = Some(amgettuple);
    amroutine.amgetbitmap = Some(amgetbitmap);
    amroutine.amendscan = Some(amendscan);

    amroutine
}

extension_sql! {r#"
    CREATE OR REPLACE FUNCTION amhandler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'amhandler_wrapper';
    CREATE ACCESS METHOD zombodb TYPE INDEX HANDLER zdb.amhandler;
"#}

#[pg_guard]
extern "C" fn amvalidate(opclassoid: pg_sys::Oid) -> bool {
    true
}

#[pg_guard]
extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    0 as *mut pg_sys::IndexBuildResult
}

#[pg_guard]
extern "C" fn ambuildempty(index_relation: pg_sys::Relation) {}

#[pg_guard]
extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    heap_relation: pg_sys::Relation,
    check_unique: pg_sys::IndexUniqueCheck,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    false
}

#[pg_guard]
extern "C" fn ambulkdelete(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
    callback: pg_sys::IndexBulkDeleteCallback,
    callback_state: *mut ::std::os::raw::c_void,
) -> *mut pg_sys::IndexBulkDeleteResult {
    0 as *mut pg_sys::IndexBulkDeleteResult
}

#[pg_guard]
extern "C" fn amvacuumcleanup(
    info: *mut pg_sys::IndexVacuumInfo,
    stats: *mut pg_sys::IndexBulkDeleteResult,
) -> *mut pg_sys::IndexBulkDeleteResult {
    0 as *mut pg_sys::IndexBulkDeleteResult
}

#[pg_guard]
extern "C" fn amcostestimate(
    root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
}

#[pg_guard]
extern "C" fn amoptions(reloptions: pg_sys::Datum, validate: bool) -> *mut pg_sys::bytea {
    0 as *mut pg_sys::bytea
}

#[pg_guard]
extern "C" fn ambeginscan(
    index_relation: pg_sys::Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> pg_sys::IndexScanDesc {
    0 as pg_sys::IndexScanDesc
}

#[pg_guard]
extern "C" fn amrescan(
    scan: pg_sys::IndexScanDesc,
    keys: pg_sys::ScanKey,
    nkeys: ::std::os::raw::c_int,
    orderbys: pg_sys::ScanKey,
    norderbys: ::std::os::raw::c_int,
) {
}

#[pg_guard]
extern "C" fn amgettuple(scan: pg_sys::IndexScanDesc, direction: pg_sys::ScanDirection) -> bool {
    false
}

#[pg_guard]
extern "C" fn amgetbitmap(scan: pg_sys::IndexScanDesc, tbm: *mut pg_sys::TIDBitmap) -> i64 {
    0
}

#[pg_guard]
extern "C" fn amendscan(scan: pg_sys::IndexScanDesc) {}
