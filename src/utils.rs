use pgx::{pg_sys, PgMemoryContexts, PgRelation, PgTupleDesc};

pub fn lookup_zdb_index_tupdesc(indexrel: &PgRelation) -> PgTupleDesc<'static> {
    let tupdesc = indexrel.tuple_desc();

    let typid = tupdesc
        .get(0)
        .expect("no attribute #0 on tupledesc")
        .type_oid()
        .value();
    let typmod = tupdesc
        .get(0)
        .expect("no attribute #0 on tupledesc")
        .type_mod();

    // lookup the tuple descriptor for the rowtype we're *indexing*, rather than
    // using the tuple descriptor for the index definition itself
    PgMemoryContexts::TopTransactionContext.switch_to(|| unsafe {
        PgTupleDesc::from_pg_is_copy(pg_sys::lookup_rowtype_tupdesc_copy(typid, typmod))
    })
}
