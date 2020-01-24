use pgx::*;

#[pg_guard]
pub extern "C" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    _path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    _index_startup_cost: *mut pg_sys::Cost,
    _index_total_cost: *mut pg_sys::Cost,
    _index_selectivity: *mut pg_sys::Selectivity,
    _index_correlation: *mut f64,
    _index_pages: *mut f64,
) {
}

#[pg_guard]
pub extern "C" fn amoptions(_reloptions: pg_sys::Datum, _validate: bool) -> *mut pg_sys::bytea {
    0 as *mut pg_sys::bytea
}
