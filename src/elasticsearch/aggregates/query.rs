use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;

#[pg_extern(immutable, parallel_safe)]
fn query(index: PgRelation, query: ZDBQuery) -> impl Iterator<Item = pg_sys::ItemPointerData> {
    let es = Elasticsearch::new(&index);
    let result = es
        .open_search(query.prepare(&index, None).0)
        .execute()
        .unwrap_or_else(|e| panic!("{}", e));

    result
        .into_iter()
        .map(|(_score, ctid, _fields, _highlights)| {
            let mut tid = pg_sys::ItemPointerData::default();
            u64_to_item_pointer(ctid, &mut tid);
            tid
        })
}

#[pg_extern(
    immutable,
    parallel_safe,
    sql = "
        CREATE OR REPLACE FUNCTION query_raw(index regclass, query zdbquery)
            RETURNS SETOF tid SET zdb.ignore_visibility = true
            IMMUTABLE STRICT ROWS 2500 LANGUAGE c AS 'MODULE_PATHNAME', 'query_raw_wrapper';
    "
)]
fn query_raw(index: PgRelation, query: ZDBQuery) -> impl Iterator<Item = pg_sys::ItemPointerData> {
    self::query(index, query)
}
