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
    _root: Internal<pg_sys::PlannerInfo>,
    _operator_oid: pg_sys::Oid,
    _args: Internal<pg_sys::List>,
    _var_relid: i32,
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
