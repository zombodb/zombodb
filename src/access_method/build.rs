use pgrx::*;

use crate::access_method::options::ZDBIndexOptions;
use crate::access_method::triggers::create_triggers;
use crate::elasticsearch::Elasticsearch;
use crate::executor_manager::{get_executor_manager, BulkContext};
use crate::gucs::ZDB_LOG_LEVEL;
use crate::json::builder::JsonBuilder;
use crate::mapping::{categorize_tupdesc, generate_default_mapping, CategorizedAttribute};
use crate::utils::{count_non_shadow_zdb_indices, lookup_zdb_index_tupdesc};

struct BuildState<'a> {
    bulk: &'a mut BulkContext,
    memcxt: PgMemoryContexts,
}

impl<'a> BuildState<'a> {
    fn new(bulk: &'a mut BulkContext) -> Self {
        BuildState {
            bulk,
            memcxt: PgMemoryContexts::new("zombodb build context"),
        }
    }
}

#[pg_guard]
pub extern "C" fn ambuild(
    heaprel: pg_sys::Relation,
    indexrel: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let heap_relation = unsafe { PgRelation::from_pg(heaprel) };
    let index_relation = unsafe { PgRelation::from_pg(indexrel) };

    if ZDBIndexOptions::from_relation_no_lookup(&index_relation, None).is_shadow_index() {
        // nothing for us to do for a shadow index
        return unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0().into_pg() };
    }

    unsafe {
        if count_non_shadow_zdb_indices(&heap_relation, &index_relation) != 0 {
            panic!("Relations can only have one non-shadow ZomboDB index");
        } else if !index_info
            .as_ref()
            .expect("index_info is null")
            .ii_Predicate
            .is_null()
        {
            panic!("ZomboDB indices cannot contain WHERE clauses");
        } else if index_info
            .as_ref()
            .expect("index_info is null")
            .ii_Concurrent
        {
            panic!("ZomboDB indices cannot be created CONCURRENTLY");
        }
    }

    let elasticsearch = Elasticsearch::new(&index_relation);
    let tupdesc = lookup_zdb_index_tupdesc(&index_relation);

    let mut mapping = generate_default_mapping(&heap_relation);
    let _ = categorize_tupdesc(&tupdesc, &heap_relation, Some(&mut mapping));

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
        if let Err(e) = delete_on_abort.execute() {
            // we can't panic here b/c we're already in the ABORT stage
            warning!(
                "failed to delete Elasticsearch index on transaction abort: {:?}",
                e
            )
        }
    });

    let ntuples = do_heap_scan(index_info, &heap_relation, &index_relation, &elasticsearch);

    // update the index settings, such as refresh_interval and number of replicas
    elasticsearch
        .update_settings()
        .execute()
        .expect("failed to update index settings after build");

    // ensure the index is added to its alias defined during CREATE INDEX
    elasticsearch
        .add_alias(elasticsearch.alias_name())
        .execute()
        .expect("failed to add index to alias during CREATE INDEX");

    // create the triggers we need on the table to which this index is attached
    if !heap_relation.is_matview() {
        create_triggers(&index_relation);
    }

    let mut result = unsafe { PgBox::<pg_sys::IndexBuildResult>::alloc0() };
    result.heap_tuples = ntuples as f64;
    result.index_tuples = ntuples as f64;

    result.into_pg()
}

fn do_heap_scan<'a>(
    index_info: *mut pg_sys::IndexInfo,
    heap_relation: &'a PgRelation,
    index_relation: &'a PgRelation,
    elasticsearch: &Elasticsearch,
) -> usize {
    let bulk_context = get_executor_manager().checkout_bulk_context(index_relation.oid());
    let mut state = BuildState::new(bulk_context);

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation.as_ptr(),
            index_relation.as_ptr(),
            index_info,
            Some(build_callback),
            &mut state,
        );
    }

    let (ntuples, nrequests) = state.bulk.es_bulk_request.totals();

    ZDB_LOG_LEVEL.get().log(&format!(
        "[zombodb] indexed {} rows to {} in {} requests",
        ntuples,
        elasticsearch.base_url(),
        nrequests
    ));

    ntuples
}

#[pg_guard]
pub extern "C" fn ambuildempty(_index_relation: pg_sys::Relation) {}

#[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12", feature = "pg13"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, heap_tid)
}

#[cfg(any(feature = "pg14", feature = "pg15"))]
#[pg_guard]
pub unsafe extern "C" fn aminsert(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_unchanged: bool,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    aminsert_internal(index_relation, values, heap_tid)
}

#[inline(always)]
unsafe fn aminsert_internal(
    index_relation: pg_sys::Relation,
    values: *mut pg_sys::Datum,
    heap_tid: pg_sys::ItemPointer,
) -> bool {
    let index_relation = PgRelation::from_pg(index_relation);
    let bulk = get_executor_manager().checkout_bulk_context(index_relation.oid());
    if bulk.is_shadow {
        // shadow indexes don't do anything
        return false;
    }

    let values = std::slice::from_raw_parts(values, 1);
    let builder = row_to_json(values[0], bulk);
    let cmin = pg_sys::GetCurrentCommandId(true);
    let cmax = cmin;
    let xmin = xid_to_64bit(pg_sys::GetCurrentTransactionId());
    let xmax = pg_sys::InvalidTransactionId as u64;

    bulk.es_bulk_request
        .insert(*heap_tid, cmin, cmax, xmin, xmax, builder)
        .expect("Unable to send tuple for insert");

    true
}

#[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12"))]
#[pg_guard]
unsafe extern "C" fn build_callback(
    _index: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    let htup = htup.as_ref().unwrap();

    build_callback_internal(htup.t_self, values, state);
}

#[cfg(any(feature = "pg13", feature = "pg14", feature = "pg15"))]
#[pg_guard]
unsafe extern "C" fn build_callback(
    _index: pg_sys::Relation,
    ctid: pg_sys::ItemPointer,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    build_callback_internal(*ctid, values, state);
}

#[inline(always)]
unsafe extern "C" fn build_callback_internal(
    ctid: pg_sys::ItemPointerData,
    values: *mut pg_sys::Datum,
    state: *mut std::os::raw::c_void,
) {
    check_for_interrupts!();

    let state = (state as *mut BuildState).as_mut().unwrap();

    let mut old_context = state.memcxt.set_as_current();

    let values = std::slice::from_raw_parts(values, 1);
    let builder = row_to_json(values[0], &state.bulk);

    let cmin = pg_sys::FirstCommandId;
    let cmax = cmin;

    let xmin = pg_sys::FirstNormalTransactionId as u64;
    let xmax = pg_sys::InvalidTransactionId as u64;

    state
        .bulk
        .es_bulk_request
        .insert(ctid, cmin, cmax, xmin, xmax, builder)
        .expect("Unable to send tuple for insert");

    old_context.set_as_current();
    state.memcxt.reset();
}

unsafe fn row_to_json(row: pg_sys::Datum, bulk: &BulkContext) -> JsonBuilder {
    let mut builder = JsonBuilder::new(bulk.attributes.len());

    for (attr, datum) in decon_row(bulk, row)
        .filter(|item| item.is_some())
        .map(|item| item.unwrap())
    {
        (attr.conversion_func)(&mut builder, attr.attname.clone(), datum, attr.typoid);
    }

    builder
}

#[inline]
unsafe fn decon_row<'a>(
    bulk: &'a BulkContext,
    row: pg_sys::Datum,
) -> impl std::iter::Iterator<Item = Option<(&'a CategorizedAttribute, pg_sys::Datum)>> + 'a {
    let td =
        pg_sys::pg_detoast_datum(row.cast_mut_ptr::<pg_sys::varlena>()) as pg_sys::HeapTupleHeader;
    let mut tmptup = pg_sys::HeapTupleData {
        t_len: varsize(td as *mut pg_sys::varlena) as u32,
        t_self: Default::default(),
        t_tableOid: pg_sys::Oid::INVALID,
        t_data: td,
    };

    let mut datums = vec![pg_sys::Datum::from(0 as usize); bulk.natts];
    let mut nulls = vec![false; bulk.natts];

    pg_sys::heap_deform_tuple(
        &mut tmptup,
        bulk.tupdesc.as_ptr(),
        datums.as_mut_ptr(),
        nulls.as_mut_ptr(),
    );

    let mut drop_cnt = 0;
    (0..bulk.natts).into_iter().map(move |idx| {
        let is_dropped = *bulk.dropped.get(idx).unwrap() == true;

        if is_dropped {
            drop_cnt += 1;
            None
        } else if nulls[idx] {
            None
        } else {
            Some((&bulk.attributes[idx - drop_cnt], *&datums[idx]))
        }
    })
}
