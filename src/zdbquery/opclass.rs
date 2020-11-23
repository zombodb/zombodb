use crate::elasticsearch::Elasticsearch;
use crate::executor_manager::get_executor_manager;
use crate::gucs::ZDB_DEFAULT_ROW_ESTIMATE;
use crate::utils::find_zdb_index;
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
    let heap_oid = match query_state.lookup_heap_oid_for_first_field(*query_desc, fcinfo) {
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
        panic!("lhs of anyelement_cmpfunc is not a tid");
    };

    match tid {
        Some(tid) => {
            let lookup = pg_func_extra(fcinfo, || {
                let heap_relation =
                    PgRelation::with_lock(heap_oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);
                let index = find_zdb_index(&heap_relation, true).unwrap();
                let es = Elasticsearch::new(&index);
                let search = es
                    .open_search(query.prepare(&index, None).0)
                    .execute()
                    .expect("failed to execute search");

                let mut lookup = HashSet::with_capacity(search.len());
                for (score, ctid, _, highlights) in search.into_iter() {
                    check_for_interrupts!();

                    // remember the score, globaly
                    let (_, qstate) = get_executor_manager().peek_query_state().unwrap();
                    qstate.add_score(heap_oid, ctid, score);
                    qstate.add_highlight(heap_oid, ctid, highlights);

                    // remember this ctid for this function context
                    lookup.insert(ctid);
                }
                lookup
            });

            lookup.contains(&tid)
        }
        None => false,
    }
}

#[pg_extern(immutable, parallel_safe)]
fn restrict(
    root: Internal<pg_sys::PlannerInfo>,
    _operator_oid: pg_sys::Oid,
    args: Internal<pg_sys::List>,
    var_relid: i32,
) -> f64 {
    let args = PgList::<pg_sys::Node>::from_pg(args.0.as_ptr());
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

    if is_a(left, pg_sys::NodeTag_T_Var) {
        let mut ldata = pg_sys::VariableStatData::default();

        unsafe {
            pg_sys::examine_variable(root.0.as_ptr(), left, var_relid, &mut ldata);

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
        if is_a(right, pg_sys::NodeTag_T_Const) {
            let rconst = PgBox::from_pg(right as *mut pg_sys::Const);

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
                        let index_relation = find_zdb_index(&heap_relation, true).unwrap();

                        let elasticsearch = Elasticsearch::new(&index_relation);
                        count_estimate = elasticsearch
                            .raw_count(zdbquery.prepare(&index_relation, None).0)
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

extension_sql! {r#"
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

"#}
