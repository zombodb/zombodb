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
    result_mut.heap_tuples = state.ntuples as f64;
    result_mut.index_tuples = state.ntuples as f64;

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
    let index_ref = index.as_ref().unwrap();
    let values = Array::<pg_sys::Datum>::over(
        values,
        isnull,
        index.as_ref().unwrap().rd_att.as_ref().unwrap().natts as usize,
    );

    let json_datum =
        direct_function_call::<&str>(pg_sys::row_to_json, vec![values.get(0).unwrap()]);

    // the index should point to a row type, so lets lookup the tuple descriptor for that
    let tupdesc = pg_sys::lookup_rowtype_tupdesc(
        index_ref.rd_att.as_ref().unwrap().tdtypeid,
        index_ref.rd_att.as_ref().unwrap().tdtypmod,
    );

    row_to_json(values.get(0).unwrap().unwrap(), tupdesc);
    state.ntuples += 1;

    info!("build callback: {}", json_datum.unwrap());
}

fn row_to_json(row: pg_sys::Datum, tupdesc: pg_sys::TupleDesc) {
    let td =
        unsafe { pg_sys::pg_detoast_datum(row as *mut pg_sys::varlena) } as pg_sys::HeapTupleHeader;
    let tuple = pg_sys::HeapTupleData {
        t_len: unsafe { varsize(td as *mut pg_sys::varlena) } as u32,
        t_self: pg_sys::ItemPointerData {
            ip_blkid: pg_sys::BlockIdData { bi_hi: 0, bi_lo: 0 },
            ip_posid: 0,
        },
        t_tableOid: 0,
        t_data: td,
    };

    let tupdesc_ref = unsafe { tupdesc.as_ref() }.unwrap();
    for i in 1..=tupdesc_ref.natts as u32 {
        let datum = heap_getattr_datum_ex(&tuple, i, tupdesc);
        info!("#{} of {}: {:?}", i, tupdesc_ref.natts, datum);
    }
}
