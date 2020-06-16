use pgx::*;

#[pg_guard(immutable, parallel_safe)]
pub unsafe extern "C" fn amcostestimate(
    _root: *mut pg_sys::PlannerInfo,
    path: *mut pg_sys::IndexPath,
    _loop_count: f64,
    index_startup_cost: *mut pg_sys::Cost,
    index_total_cost: *mut pg_sys::Cost,
    index_selectivity: *mut pg_sys::Selectivity,
    index_correlation: *mut f64,
    index_pages: *mut f64,
) {
    let path = path.as_ref().expect("path argument is NULL");
    let indexinfo = path.indexinfo.as_ref().expect("indexinfo in path is NULL");
    let index_relation = PgRelation::open(indexinfo.indexoid);
    let heap_relation = index_relation
        .heap_relation()
        .expect("failed to get heap relation for index");

    *index_correlation = 1.0;
    *index_startup_cost = 0.0;
    *index_pages = 0.0;
    *index_total_cost = 0.0;

    // go with the smallest already-calculated selectivity.
    // these would have been calculated in zdb_restrict()
    *index_selectivity = 1.0;
    let index_clauses = PgList::<pg_sys::IndexClause>::from_pg(path.indexclauses);
    for clause in index_clauses.iter_ptr() {
        let ri = clause
            .as_ref()
            .unwrap()
            .rinfo
            .as_ref()
            .expect("restrict info in index clause is NULL");

        if ri.norm_selec > 0f64 {
            *index_selectivity = ri.norm_selec.min(*index_selectivity);
        }
    }

    let reltuples = heap_relation.reltuples().unwrap_or(1f32) as f64;
    *index_total_cost += *index_selectivity * reltuples * pg_sys::cpu_index_tuple_cost;

    // we subtract random_page_cost from the total cost because Postgres assumes we'll read at least
    // one index page, and that's just not true for ZomboDB -- we have no pages on disk
    //
    //  Assuming default values for random_page_cost and seq_page_cost, this should always
    //  get our IndexScans set to a lower cost than a sequential scan, which we don't necessarily prefer,
    //  allowing Postgres to instead prefer to use our index for plans where it can actually use one
    *index_total_cost -= pg_sys::random_page_cost;
}
