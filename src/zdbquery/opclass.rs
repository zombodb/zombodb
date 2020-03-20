use crate::elasticsearch::Elasticsearch;
use crate::gucs::ZDB_DEFAULT_ROW_ESTIMATE;
use crate::utils::find_zdb_index;
use crate::zdbquery::ZDBQuery;
use pgx::*;

#[pg_extern(immutable)]
fn anyelement_cmpfunc(_element: AnyElement, _query: ZDBQuery) -> bool {
    ereport(
        PgLogLevel::ERROR,
        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        "anyelement_cmpfunc called in invalid context",
        file!(),
        line!(),
        column!(),
    );
    false
}

#[pg_extern(immutable)]
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

            heap_relation = Some(PgRelation::open(heaprel_id));
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

                    let estimate = zdbquery.row_estimate();

                    if estimate >= 1 {
                        // just the estimate assigned to the query
                        count_estimate = estimate as u64;
                    } else {
                        // ask Elasticsearch to estimate our selectivity
                        let index_relation = find_zdb_index(&heap_relation);

                        let elasticsearch = Elasticsearch::new(&index_relation);
                        count_estimate = elasticsearch
                            .raw_count(zdbquery)
                            .execute()
                            .expect("failed to estimate selectivity");
                    }
                }
            }
        }

        reltuples = heap_relation.reltuples().unwrap_or(1f32) as f64;
    }

    (count_estimate as f64 / reltuples.max(1f64)).max(0f64)
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
