use crate::elasticsearch::{Elasticsearch, ElasticsearchBulkRequest};
use pgx::*;
use std::ops::DerefMut;

struct BuildState {
    ntuples: usize,
    bulk: ElasticsearchBulkRequest,
}

#[pg_guard]
pub extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let mut result = PgBox::<pg_sys::IndexBuildResult>::alloc0();
    let result_mut = result.deref_mut();

    let mut state = BuildState {
        ntuples: 0,
        bulk: Elasticsearch::new("http://localhost:9200/", "test_index").start_bulk(),
    };

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation,
            index_relation,
            index_info,
            Some(build_callback),
            &mut state,
        );
    }

    info!("Waiting to finish");
    match state.bulk.wait_for_completion() {
        Ok(cnt) => info!("indexed {} tuples", cnt),
        Err(e) => panic!("{:?}", e),
    }

    info!("ntuples={}", state.ntuples);
    result_mut.heap_tuples = state.ntuples as f64;
    result_mut.index_tuples = state.ntuples as f64;

    result.into_pg()
}

#[pg_guard]
pub extern "C" fn ambuildempty(_index_relation: pg_sys::Relation) {}

#[pg_guard]
pub extern "C" fn aminsert(
    _index_relation: pg_sys::Relation,
    _values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    info!("aminsert");
    false
}

unsafe extern "C" fn build_callback(
    index: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
    isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let htup_header = htup.as_ref().expect("received null HeapTuple").t_data;
    let mut state = PgBox::from_pg(state as *mut BuildState);
    let index_ref = index.as_ref().unwrap();
    let values = Array::<pg_sys::Datum>::over(
        values,
        isnull,
        index_ref.rd_att.as_ref().unwrap().natts as usize,
    );

    let json_datum =
        direct_function_call::<&str>(pg_sys::row_to_json, vec![values.get(0).unwrap()])
            .expect("couldn't get json datum");

    let cmin = pg_sys::HeapTupleHeaderGetRawCommandId(htup_header)
        .expect("unable to get tuple raw command id");
    let cmax = cmin;
    let xmin = xid_to_64bit(
        pg_sys::HeapTupleHeaderGetXmin(htup_header).expect("unable to get tuple xmin"),
    );
    let xmax = pg_sys::InvalidTransactionId as u64;

    state
        .bulk
        .insert(
            htup.as_ref().expect("got null HeapTuple").t_self.clone(),
            cmin,
            cmax,
            xmin,
            xmax,
            serde_json::from_str(json_datum).expect("couldn't parse json datum"),
        )
        .expect("failed to insert");
    //    // the index should point to a row type, so lets lookup the tuple descriptor for that
    //    let tupdesc = pg_sys::lookup_rowtype_tupdesc(
    //        index_ref.rd_att.as_ref().unwrap().tdtypeid,
    //        index_ref.rd_att.as_ref().unwrap().tdtypmod,
    //    );
    //
    //    row_to_json(values.get(0).unwrap().unwrap(), tupdesc);
    state.ntuples += 1;

    //    info!("build callback: {}", json_datum.unwrap());
}
