use crate::elasticsearch::Elasticsearch;
use crate::executor_manager::get_executor_manager;
use crate::gucs::ZDB_DEFAULT_ROW_ESTIMATE;
use crate::utils::get_heap_relation_for_func_expr;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use std::collections::HashSet;

#[pg_extern(immutable, parallel_safe)]
fn anyelement_cmpfunc(
    element: AnyElement,
    query: ZDBQuery,
    fcinfo: pg_sys::FunctionCallInfo,
) -> bool {
    let (query_desc, query_state) = match get_executor_manager().peek_query_state() {
        Some((query_desc, query_state)) => (query_desc, query_state),
        None => return false,
    };
    let index_oid = match query_state.lookup_index_for_first_field(*query_desc, fcinfo) {
        Some(oid) => oid,
        None => return false,
    };

    let tid = if element.oid() == pg_sys::TIDOID {
        // use the ItemPointerData passed into us as the first argument
        Some(item_pointer_to_u64(
            unsafe { pg_sys::ItemPointerData::from_datum(element.datum(), false, element.oid()) }
                .unwrap(),
        ))
    } else {
        panic!(
            "The '==>' operator could not find a \"USING zombodb\" index that matches the left-hand-side of the expression"
        );
    };

    match tid {
        Some(tid) => unsafe {
            pg_func_extra(fcinfo, || do_seqscan(query, index_oid)).contains(&tid)
        },
        None => false,
    }
}

#[inline]
fn do_seqscan(query: ZDBQuery, index_oid: u32) -> HashSet<u64> {
    unsafe {
        let index = pg_sys::index_open(index_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        let heap = pg_sys::relation_open(
            index.as_ref().unwrap().rd_index.as_ref().unwrap().indrelid,
            pg_sys::AccessShareLock as pg_sys::LOCKMODE,
        );

        let mut keys = PgBox::<pg_sys::ScanKeyData>::alloc0();
        keys.sk_argument = query.into_datum().unwrap();

        let scan = pg_sys::index_beginscan(heap, index, pg_sys::GetTransactionSnapshot(), 1, 0);
        pg_sys::index_rescan(scan, keys.into_pg(), 1, std::ptr::null_mut(), 0);

        let mut lookup = HashSet::new();
        loop {
            check_for_interrupts!();

            #[cfg(any(feature = "pg10", feature = "pg11"))]
            let tid = {
                let htup = pg_sys::index_getnext(scan, pg_sys::ScanDirection_ForwardScanDirection);
                if htup.is_null() {
                    break;
                }
                item_pointer_to_u64(htup.as_ref().unwrap().t_self)
            };

            #[cfg(any(feature = "pg12", feature = "pg13", feature = "pg14"))]
            let tid = {
                let slot = pg_sys::MakeSingleTupleTableSlot(
                    heap.as_ref().unwrap().rd_att,
                    &pg_sys::TTSOpsBufferHeapTuple,
                );

                if !pg_sys::index_getnext_slot(
                    scan,
                    pg_sys::ScanDirection_ForwardScanDirection,
                    slot,
                ) {
                    pg_sys::ExecDropSingleTupleTableSlot(slot);
                    break;
                }

                let tid = item_pointer_to_u64(slot.as_ref().unwrap().tts_tid);
                pg_sys::ExecDropSingleTupleTableSlot(slot);
                tid
            };
            lookup.insert(tid);
        }
        pg_sys::index_endscan(scan);
        pg_sys::index_close(index, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        pg_sys::relation_close(heap, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
        lookup
    }
}

#[pg_extern(immutable, parallel_safe)]
fn restrict(
    planner_info: Internal, // <pg_sys::PlannerInfo>,
    _operator_oid: pg_sys::Oid,
    args: Internal, // <pg_sys::List>,
    var_relid: i32,
) -> f64 {
    let root = unsafe { planner_info.get_mut::<pg_sys::PlannerInfo>().unwrap() as *mut _ };
    let args = unsafe { args.get_mut::<pg_sys::List>().unwrap() as *mut _ };
    let args = unsafe { PgList::<pg_sys::Node>::from_pg(args) };
    let left = args.get_ptr(0);
    let right = args.get_ptr(1);

    if left.is_none() {
        panic!("left argument is null");
    } else if right.is_none() {
        panic!("right argument is null");
    }

    let mut reltuples = 1f64;
    let mut count_estimate = 1u64;
    let left = left.unwrap();
    let right = right.unwrap();
    let mut heap_relation = None;

    if unsafe { is_a(left, pg_sys::NodeTag_T_FuncExpr) } {
        unsafe {
            let func = PgBox::<pg_sys::FuncExpr>::from_pg(left as *mut pg_sys::FuncExpr);
            let planner_info = planner_info.get::<pg_sys::PlannerInfo>().unwrap();
            let query = PgBox::<pg_sys::Query>::from_pg(planner_info.parse);
            let heap = get_heap_relation_for_func_expr(None, &func, &query);

            heap_relation = Some(heap);
        }
    } else if unsafe { is_a(left, pg_sys::NodeTag_T_Var) } {
        let mut ldata = pg_sys::VariableStatData::default();

        unsafe {
            pg_sys::examine_variable(root, left, var_relid, &mut ldata);

            let type_oid = ldata.vartype;
            let tce: PgBox<pg_sys::TypeCacheEntry> =
                PgBox::from_pg(pg_sys::lookup_type_cache(type_oid, 0));
            let heaprel_id = tce.typrelid;

            if heaprel_id == pg_sys::InvalidOid {
                heap_relation = None;
            } else {
                heap_relation = Some(PgRelation::open(heaprel_id));
            }

            // free the ldata struct
            if !ldata.statsTuple.is_null() {
                (ldata.freefunc.unwrap())(ldata.statsTuple);
            }
        }
    }

    if let Some(heap_relation) = heap_relation {
        if unsafe { is_a(right, pg_sys::NodeTag_T_Const) } {
            let rconst = unsafe { PgBox::from_pg(right as *mut pg_sys::Const) };

            unsafe {
                if pg_sys::type_is_array(rconst.consttype) {
                    count_estimate = ZDB_DEFAULT_ROW_ESTIMATE.get() as u64;
                } else {
                    let zdbquery: ZDBQuery = ZDBQuery::from_datum(
                        rconst.constvalue,
                        rconst.constisnull,
                        rconst.consttype,
                    )
                    .expect("rhs of ==> is NULL");

                    let estimate = zdbquery
                        .row_estimate()
                        .min(zdbquery.limit().unwrap_or(std::i64::MAX as u64) as i64);

                    if estimate >= 1 {
                        // use the estimate assigned to the query
                        count_estimate = estimate as u64;
                    } else {
                        // ask Elasticsearch to estimate our selectivity
                        let es = Elasticsearch::new(&heap_relation);
                        count_estimate = es
                            .raw_count(zdbquery.prepare(&es.index_relation(), None).0)
                            .execute()
                            .expect("failed to estimate selectivity");
                    }
                }
            }
        }

        reltuples = heap_relation.reltuples().unwrap_or(1f32) as f64;
    }

    let reltuples = reltuples.max(count_estimate as f64).max(1f64);

    count_estimate as f64 / reltuples
}

extension_sql!(
    r#"
CREATE OPERATOR pg_catalog.==> (
    PROCEDURE = anyelement_cmpfunc,
    RESTRICT = restrict,
    LEFTARG = anyelement,
    RIGHTARG = zdbquery
);

CREATE OPERATOR CLASS anyelement_zdb_ops DEFAULT FOR TYPE anyelement USING zombodb AS
    OPERATOR 1 pg_catalog.==>(anyelement, zdbquery),
--    OPERATOR 2 pg_catalog.==|(anyelement, zdbquery[]),
--    OPERATOR 3 pg_catalog.==&(anyelement, zdbquery[]),
--    OPERATOR 4 pg_catalog.==!(anyelement, zdbquery[]),
    STORAGE anyelement;

"#,
    name = "zdb_ops_anyelement_operator"
);
