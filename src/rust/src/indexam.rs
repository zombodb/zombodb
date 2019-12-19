#![allow(unused_variables)]

use pg_bridge::*;
use pg_sys::*;

#[pg_extern]
pub unsafe extern "C" fn zdb_amhandler(_fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    let mut amroutine = make_node::<pg_sys::IndexAmRoutine>(pg_sys::NodeTag_T_IndexAmRoutine);

    info!("zdb_amhandler");

    (*amroutine).amstrategies = 4;
    (*amroutine).amsupport = 0;
    (*amroutine).amcanorder = false;
    (*amroutine).amcanorderbyop = false;
    (*amroutine).amcanbackward = false;
    (*amroutine).amcanunique = false;
    (*amroutine).amcanmulticol = true;
    (*amroutine).amoptionalkey = false;
    (*amroutine).amsearcharray = true;
    (*amroutine).amsearchnulls = false;
    (*amroutine).amstorage = false;
    (*amroutine).amclusterable = false;
    (*amroutine).ampredlocks = false;
    (*amroutine).amcanparallel = false;

    (*amroutine).amkeytype = pg_sys::InvalidOid;

    (*amroutine).amvalidate = Some(zdb_amvalidate);
    (*amroutine).ambuild = Some(ambuild);
    (*amroutine).ambuildempty = Some(ambuildempty);
    (*amroutine).aminsert = Some(aminsert);
    (*amroutine).ambulkdelete = Some(ambulkdelete);
    (*amroutine).amvacuumcleanup = Some(amvacuumcleanup);
    (*amroutine).amcanreturn = None;
    (*amroutine).amcostestimate = Some(amcostestimate);
    (*amroutine).amoptions = Some(amoptions);
    (*amroutine).amproperty = None;
    (*amroutine).ambeginscan = Some(ambeginscan);
    (*amroutine).amrescan = Some(amrescan);
    (*amroutine).amgettuple = Some(amgettuple);
    (*amroutine).amgetbitmap = Some(amgetbitmap);
    (*amroutine).amendscan = Some(amendscan);

    (*amroutine).ammarkpos = None;
    (*amroutine).amrestrpos = None;
    (*amroutine).amestimateparallelscan = None;
    (*amroutine).aminitparallelscan = None;
    (*amroutine).amparallelrescan = None;

    amroutine as pg_sys::Datum
}

#[pg_guard]
unsafe extern "C" fn zdb_amvalidate(opclassoid: Oid) -> bool {
    info!("zdbamvalidate");
    true
}

#[pg_guard]
unsafe extern "C" fn ambuild(
    heap_relation: Relation,
    index_relation: Relation,
    index_info: *mut IndexInfo,
) -> *mut IndexBuildResult {
    info!("ambuild");
    let result = palloc0_struct::<pg_sys::IndexBuildResult>();

    return result;
}

#[pg_guard]
unsafe extern "C" fn ambuildempty(index_relation: Relation) {
    info!("ambuildempty");
}

#[pg_guard]
unsafe extern "C" fn aminsert(
    index_relation: Relation,
    values: *mut Datum,
    isnull: *mut bool,
    heap_tid: ItemPointer,
    heap_relation: Relation,
    check_unique: IndexUniqueCheck,
    index_info: *mut IndexInfo,
) -> bool {
    info!("aminsert");
    false
}

#[pg_guard]
unsafe extern "C" fn ambulkdelete(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut ::std::os::raw::c_void,
) -> *mut IndexBulkDeleteResult {
    info!("ambulkdelete");
    stats
}

#[pg_guard]
unsafe extern "C" fn amvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    info!("amvacuumcleanup");
    stats
}

#[pg_guard]
unsafe extern "C" fn amcostestimate(
    root: *mut PlannerInfo,
    path: *mut IndexPath,
    loop_count: f64,
    index_startup_cost: *mut Cost,
    index_total_cost: *mut Cost,
    index_selectivity: *mut Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    info!("amcostestimate");
}

#[pg_guard]
unsafe extern "C" fn amoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    info!("amoptions");
    0 as *mut bytea
}

#[pg_guard]
unsafe extern "C" fn ambeginscan(
    index_relation: Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> IndexScanDesc {
    info!("ambeginscan");
    let result = palloc0_struct::<pg_sys::IndexScanDescData>();

    result
}

#[pg_guard]
unsafe extern "C" fn amrescan(
    scan: IndexScanDesc,
    keys: ScanKey,
    nkeys: ::std::os::raw::c_int,
    orderbys: ScanKey,
    norderbys: ::std::os::raw::c_int,
) {
    info!("amresscan");
}

#[pg_guard]
unsafe extern "C" fn amgettuple(scan: IndexScanDesc, direction: ScanDirection) -> bool {
    info!("amgettuple");
    false
}

#[pg_guard]
unsafe extern "C" fn amgetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    info!("amgetbitmap");
    0
}

#[pg_guard]
unsafe extern "C" fn amendscan(scan: IndexScanDesc) {
    info!("amendscan");
}
