use pgx::*;
use std::ops::DerefMut;

struct BuildState {
    ntuples: usize,
}

impl BuildState {
    fn from_ptr(ptr: *mut std::os::raw::c_void) -> PgBox<BuildState> {
        PgBox::<BuildState>::from_pg(ptr as *mut BuildState)
    }
}

#[pg_guard]
pub extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let mut result = PgBox::<pg_sys::IndexBuildResult>::alloc0();
    let result_mut = result.deref_mut();

    let mut state = BuildState { ntuples: 0 };

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation,
            index_relation,
            index_info,
            Some(build_callback),
            &mut state,
        );
    }

    info!("ntuples={}", state.ntuples);
    result.into_pg()
}

#[pg_guard]
pub extern "C" fn ambuildempty(index_relation: pg_sys::Relation) {}

#[pg_guard]
pub extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    heap_relation: pg_sys::Relation,
    check_unique: pg_sys::IndexUniqueCheck,
    index_info: *mut pg_sys::IndexInfo,
) -> bool {
    info!("aminsert");
    false
}

unsafe extern "C" fn build_callback(
    index: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let mut state = BuildState::from_ptr(state);
    let state_mut = state.as_mut().unwrap();
    let values = Array::<pg_sys::Datum>::over(
        values,
        isnull,
        index.as_ref().unwrap().rd_att.as_ref().unwrap().natts as usize,
    );

    let json_datum =
        direct_function_call::<&str>(pg_sys::row_to_json, vec![values.get(0).unwrap()]);

    state.ntuples += 1;

    info!("build callback: {}", json_datum.unwrap());
}
