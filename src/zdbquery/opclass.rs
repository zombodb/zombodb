use crate::zdbquery::ZDBQuery;
use pgx::*;
use std::any::Any;

#[pg_extern(immutable)]
fn anyelement_cmpfunc(element: TypedDatum, query: ZDBQuery) -> bool {
    info!("anyelement_cmpfunc: {:?}", element);
    true
}

#[pg_extern(immutable)]
fn restrict(
    root: Internal<pg_sys::PlannerInfo>,
    operator_oid: pg_sys::Oid,
    args: Internal<pg_sys::List>,
    var_relid: i32,
) -> f64 {
    info!("in restrict");
    0f64
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
