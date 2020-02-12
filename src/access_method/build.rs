use crate::elasticsearch::{Elasticsearch, ElasticsearchBulkRequest};
use crate::gucs::ZDB_LOG_LEVEL;
use crate::json::builder::JsonBuilder;
use crate::mapping::{categorize_tupdesc, generate_default_mapping, CategorizedAttribute};
use crate::utils::lookup_zdb_index_tupdesc;
use pgx::*;

struct BuildState<'a> {
    bulk: ElasticsearchBulkRequest,
    tupdesc: &'a PgBox<pg_sys::TupleDescData>,
    attributes: Vec<CategorizedAttribute<'a>>,
}

impl<'a> BuildState<'a> {
    fn new(
        bulk: ElasticsearchBulkRequest,
        tupdesc: &'a PgBox<pg_sys::TupleDescData>,
        attributes: Vec<CategorizedAttribute<'a>>,
    ) -> Self {
        BuildState {
            bulk,
            tupdesc,
            attributes,
        }
    }
}

#[pg_guard]
pub extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let heap_relation = PgBox::from_pg(heap_relation);
    let index_relation = PgBox::from_pg(index_relation);
    let tupdesc = lookup_zdb_index_tupdesc(&index_relation);

    let mut mapping = generate_default_mapping();
    let attributes = categorize_tupdesc(&tupdesc, &mut mapping);
    let elasticsearch = Elasticsearch::new(&heap_relation, &index_relation);

    // delete any existing Elasticsearch index with the same name as this one we're about to create
    elasticsearch
        .delete_index()
        .execute()
        .expect("failed to delete existing Elasticsearch index");

    // create the new index
    elasticsearch
        .create_index(serde_json::to_value(&mapping).expect("failed to generate mapping"))
        .execute()
        .expect("failed to create new Elasticsearch index");

    // register a callback to delete the newly-created ES index if our transaction aborts
    let delete_on_abort = elasticsearch.delete_index();
    register_xact_callback(PgXactCallbackEvent::Abort, move || {
        delete_on_abort
            .execute()
            .expect("failed to delete Elasticsearch index on transaction abort")
    });

    let mut state = BuildState::new(elasticsearch.start_bulk(), &tupdesc, attributes);

    // register an Abort callback so we can terminate early if there's an error
    let callback = register_xact_callback(PgXactCallbackEvent::Abort, state.bulk.terminate());
    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation.as_ptr(),
            index_relation.as_ptr(),
            index_info,
            Some(build_callback),
            &mut state,
        );
    }
    if tupdesc.tdrefcount >= 0 {
        unsafe {
            pg_sys::DecrTupleDescRefCount(tupdesc.as_ptr());
        }
    }

    let ntuples = state.bulk.finish().expect("Failed to finalize indexing");
    elog(
        ZDB_LOG_LEVEL.get().log_level(),
        &format!("Indexed {} rows to {}", ntuples, elasticsearch.base_url()),
    );

    // our work with Elasticsearch is done, so we can unregister our Abort callback
    callback.unregister_callback();

    let mut result = PgBox::<pg_sys::IndexBuildResult>::alloc0();
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;

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
    _index: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    check_for_interrupts!();

    let htup = PgBox::from_pg(htup);
    let mut state = PgBox::from_pg(state as *mut BuildState);
    let values = std::slice::from_raw_parts(values, 1);
    let builder = row_to_json(values[0], &state);

    state
        .bulk
        .insert(htup.t_self, 0, 0, 0, 0, builder)
        .expect("Unable to send tuple for insert");
}

unsafe fn row_to_json<'a>(
    row: pg_sys::Datum,
    state: &PgBox<BuildState<'static>>,
) -> JsonBuilder<'a> {
    let mut builder = JsonBuilder::new(state.attributes.len());

    let datums = deconstruct_row_type(state.tupdesc, row);
    for (attr, datum) in state
        .attributes
        .iter()
        .zip(datums.iter())
        .filter(|(attr, datum)| !attr.dropped && datum.is_some())
    {
        let datum = datum.expect("found NULL datum"); // shouldn't happen b/c None datums are filtered above

        (attr.conversion_func)(&mut builder, attr.attname, datum, attr.typoid);
    }

    builder
}
