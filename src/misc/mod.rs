use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;

#[pg_extern(immutable, parallel_safe)]
fn query_tids(index: PgRelation, query: ZDBQuery) -> Vec<pg_sys::ItemPointerData> {
    let es = Elasticsearch::new(&index);
    let scroll = es
        .open_search(query.prepare())
        .execute()
        .expect("failed to execute search");
    let mut tids = Vec::new();
    for (_, tid, _, _) in scroll.into_iter() {
        let mut ipd = pg_sys::ItemPointerData::default();
        u64_to_item_pointer(tid, &mut ipd);

        tids.push(ipd);
    }
    tids
}
