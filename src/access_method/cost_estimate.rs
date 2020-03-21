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
    let path = PgBox::from_pg(path);
    let indexinfo = PgBox::from_pg(path.indexinfo);
    let index_relation = PgRelation::open(indexinfo.indexoid);
    let heap_relation = index_relation
        .heap_relation()
        .expect("failed to get heap relation for index");

    // we subtract random_page_cost from the total cost because Postgres assumes we'll read at least
    // one index page, and that's just not true for ZomboDB -- we have no pages on disk
    //
    //  Assuming default values for random_page_cost and seq_page_cost, this should always
    //  get our IndexScans set to a lower cost than a sequential scan, which we don't necessarily prefer,
    //  allowing Postgres to instead prefer to use our index for plans where it can actually use one
    if *index_total_cost > pg_sys::random_page_cost {
        *index_total_cost -= pg_sys::random_page_cost;
    }

    // go with the smallest already-calculated selectivity.
    // these would have been calculated in zdb_restrict()
    *index_selectivity = 1.0;
    let index_clauses = PgList::<pg_sys::RestrictInfo>::from_pg(path.indexclauses);
    for i in 0..index_clauses.len() {
        let ri = PgBox::from_pg(
            index_clauses
                .get_ptr(i)
                .expect("failed to get RestrictInfo from index clause list"),
        );

        if ri.norm_selec > -1.0 && ri.norm_selec < *index_selectivity {
            *index_selectivity = ri.norm_selec;
        }
    }

    *index_correlation = 1.0;
    *index_startup_cost = 0.0;
    *index_pages = 0.0;

    let reltuples = heap_relation.reltuples().unwrap_or(1f32) as f64;
    *index_total_cost += *index_selectivity * reltuples * pg_sys::cpu_index_tuple_cost;
}
