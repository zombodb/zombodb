use crate::executor_manager::get_executor_manager;
use crate::zdbquery::ZDBQuery;
use pgrx::*;

#[pg_extern(immutable, parallel_safe)]
fn score(ctid: Option<pg_sys::ItemPointerData>, fcinfo: pg_sys::FunctionCallInfo) -> f64 {
    match ctid {
        Some(ctid) => {
            let score = match get_executor_manager().peek_query_state() {
                Some((query_desc, query_state)) => {
                    query_state.lookup_index_for_first_field(*query_desc, fcinfo).map(|index_oid| query_state.get_score(index_oid, ctid))
                }
                None => None,
            };

            match score {
                Some(score) => score,
                None => {
                    panic!("zdb_score()'s argument is not a direct table ctid column reference")
                }
            }
        }
        None => 0.0f64,
    }
}

#[pg_extern(immutable, parallel_safe)]
fn want_scores(query: ZDBQuery) -> ZDBQuery {
    query.set_want_score(true)
}
