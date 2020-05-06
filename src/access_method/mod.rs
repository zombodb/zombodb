use pgx::*;

mod build;
mod cost_estimate;
pub mod options;
pub mod rewriter;
mod scan;
mod triggers;
mod vacuum;

/// ```sql
/// CREATE OR REPLACE FUNCTION amhandler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', '@FUNCTION_NAME@';
/// CREATE ACCESS METHOD zombodb TYPE INDEX HANDLER amhandler;
/// ```
#[pg_extern]
fn amhandler(_fcinfo: pg_sys::FunctionCallInfo) -> PgBox<pg_sys::IndexAmRoutine> {
    let mut amroutine = PgNodeFactory::makeIndexAmRoutine();

    amroutine.amstrategies = 4;
    amroutine.amsupport = 0;
    amroutine.amcanmulticol = true;
    amroutine.amsearcharray = true;

    amroutine.amkeytype = pg_sys::InvalidOid;

    amroutine.amvalidate = Some(amvalidate);
    amroutine.ambuild = Some(build::ambuild);
    amroutine.ambuildempty = Some(build::ambuildempty);
    amroutine.aminsert = Some(build::aminsert);
    amroutine.ambulkdelete = Some(vacuum::ambulkdelete);
    amroutine.amvacuumcleanup = Some(vacuum::amvacuumcleanup);
    amroutine.amcostestimate = Some(cost_estimate::amcostestimate);
    amroutine.amoptions = Some(options::amoptions);
    amroutine.ambeginscan = Some(scan::ambeginscan);
    amroutine.amrescan = Some(scan::amrescan);
    amroutine.amgettuple = Some(scan::amgettuple);
    amroutine.amgetbitmap = None;
    amroutine.amendscan = Some(scan::amendscan);

    amroutine
}

#[pg_guard]
pub extern "C" fn amvalidate(_opclassoid: pg_sys::Oid) -> bool {
    true
}
