#![allow(unused_variables)]

use pg_bridge::pg_sys::pg11_specific::{
    HeapScanDesc, IndexAmRoutine, IndexBuildHeapScan, IndexInfo, IndexPath, IndexVacuumInfo,
    NodeTag_T_IndexAmRoutine, PlannerInfo,
};
use pg_bridge::pg_sys::*;
use pg_bridge::*;
use pg_bridge_macros::*;
use std::os::raw::c_void;

#[pg_extern]
fn zdb_amhandler() -> PgBox<pg_sys::IndexAmRoutine> {
    info!("zdb_handler");
    let mut amroutine = unsafe { make_node::<IndexAmRoutine>(NodeTag_T_IndexAmRoutine) };

    amroutine.amstrategies = 4;
    amroutine.amsupport = 0;
    amroutine.amcanorder = false;
    amroutine.amcanorderbyop = false;
    amroutine.amcanbackward = false;
    amroutine.amcanunique = false;
    amroutine.amcanmulticol = true;
    amroutine.amoptionalkey = false;
    amroutine.amsearcharray = true;
    amroutine.amsearchnulls = false;
    amroutine.amstorage = false;
    amroutine.amclusterable = false;
    amroutine.ampredlocks = false;
    amroutine.amcanparallel = false;
    amroutine.amcaninclude = false;
    amroutine.amkeytype = InvalidOid;
    amroutine.amvalidate = Some(zdb_amvalidate);
    amroutine.ambuild = Some(ambuild);
    amroutine.ambuildempty = Some(ambuildempty);
    amroutine.aminsert = Some(aminsert);
    amroutine.ambulkdelete = Some(ambulkdelete);
    amroutine.amvacuumcleanup = Some(amvacuumcleanup);
    amroutine.amcanreturn = None;
    amroutine.amcostestimate = Some(amcostestimate);
    amroutine.amoptions = Some(amoptions);
    amroutine.amproperty = None;
    amroutine.ambeginscan = Some(ambeginscan);
    amroutine.amrescan = Some(amrescan);
    amroutine.amgettuple = Some(amgettuple);
    amroutine.amgetbitmap = Some(amgetbitmap);
    amroutine.amendscan = Some(amendscan);

    amroutine.ammarkpos = None;
    amroutine.amrestrpos = None;
    amroutine.amestimateparallelscan = None;
    amroutine.aminitparallelscan = None;
    amroutine.amparallelrescan = None;

    amroutine
}

#[pg_guard]
extern "C" fn zdb_amvalidate(opclassoid: Oid) -> bool {
    info!("zdbamvalidate");
    true
}

extern "C" fn ambuild_callback(
    index: Relation,
    htup: HeapTuple,
    values: *mut Datum,
    isnull: *mut bool,
    tuple_is_alive: bool,
    state: *mut ::std::os::raw::c_void,
) {
    let values = unsafe { std::slice::from_raw_parts(values as *const Datum, 1) };
    let ctid = (unsafe { *htup }).t_self;

    info!(
        "({}, {})",
        unsafe { item_pointer_get_block_number(&ctid) },
        unsafe { item_pointer_get_offset_number(&ctid) }
    );
}

#[pg_guard]
extern "C" fn ambuild(
    heap_relation: Relation,
    index_relation: Relation,
    index_info: *mut IndexInfo,
) -> *mut IndexBuildResult {
    info!("ambuild");
    let mut result = PgBox::<IndexBuildResult>::alloc0();

    result.heap_tuples = 12.0;

    info!("{}", result);

    unsafe {
        IndexBuildHeapScan(
            heap_relation,
            index_relation,
            index_info,
            true,
            Some(ambuild_callback),
            0 as *mut c_void,
            0 as HeapScanDesc,
        );
    }

    return result.into_pg();
}

#[pg_guard]
extern "C" fn ambuildempty(index_relation: Relation) {
    info!("ambuildempty");
}

#[pg_guard]
extern "C" fn aminsert(
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
extern "C" fn ambulkdelete(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
    callback: IndexBulkDeleteCallback,
    callback_state: *mut ::std::os::raw::c_void,
) -> *mut IndexBulkDeleteResult {
    info!("ambulkdelete");
    stats
}

#[pg_guard]
extern "C" fn amvacuumcleanup(
    info: *mut IndexVacuumInfo,
    stats: *mut IndexBulkDeleteResult,
) -> *mut IndexBulkDeleteResult {
    info!("amvacuumcleanup");
    stats
}

#[pg_guard]
extern "C" fn amcostestimate(
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
extern "C" fn amoptions(reloptions: Datum, validate: bool) -> *mut bytea {
    info!("amoptions");
    0 as *mut bytea
}

#[pg_guard]
extern "C" fn ambeginscan(
    index_relation: Relation,
    nkeys: ::std::os::raw::c_int,
    norderbys: ::std::os::raw::c_int,
) -> IndexScanDesc {
    info!("ambeginscan");
    let result = palloc0_struct::<pg_sys::IndexScanDescData>();

    result.into_pg()
}

#[pg_guard]
extern "C" fn amrescan(
    scan: IndexScanDesc,
    keys: ScanKey,
    nkeys: ::std::os::raw::c_int,
    orderbys: ScanKey,
    norderbys: ::std::os::raw::c_int,
) {
    info!("amresscan");
}

#[pg_guard]
extern "C" fn amgettuple(scan: IndexScanDesc, direction: ScanDirection) -> bool {
    info!("amgettuple");
    false
}

#[pg_guard]
extern "C" fn amgetbitmap(scan: IndexScanDesc, tbm: *mut TIDBitmap) -> int64 {
    info!("amgetbitmap");
    0
}

#[pg_guard]
extern "C" fn amendscan(scan: IndexScanDesc) {
    info!("amendscan");
}
