use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use std::convert::TryInto;

#[pg_extern(immutable, parallel_safe)]
fn count(index: PgRelation, query: ZDBQuery) -> i64 {
    let es = Elasticsearch::new(&index);

    es.count(query.prepare(&index))
        .execute()
        .expect("failed to execute count query")
        .try_into()
        .expect("count request overflowed an i64")
}

#[pg_extern(immutable, parallel_safe)]
fn raw_count(index: PgRelation, query: ZDBQuery) -> i64 {
    let es = Elasticsearch::new(&index);

    es.raw_count(query.prepare(&index))
        .execute()
        .expect("failed to execute raw count query")
        .try_into()
        .expect("count request overflowed an i64")
}
