use crate::elasticsearch::Elasticsearch;
use crate::zdbquery::ZDBQuery;
use pgx::*;
use std::convert::TryInto;

#[pg_extern(immutable, parallel_safe)]
fn count(index: PgRelation, query: ZDBQuery) -> i64 {
    let es = Elasticsearch::new(&index);

    es.count(query)
        .execute()
        .expect("failed to execute count query")
        .try_into()
        .expect("count request overflowed an i64")
}
