use crate::access_method::amvalidate;
use pgx::*;
use serde_json::Value;

pub fn find_zdb_index(heap_relation: &PgRelation) -> PgRelation {
    let index_list = PgList::<pg_sys::Oid>::from_pg(unsafe {
        pg_sys::RelationGetIndexList(heap_relation.as_ptr())
    });

    for i in 0..index_list.len() {
        let oid = index_list.get_oid(i).unwrap();
        let index_relation: *mut pg_sys::RelationData =
            unsafe { pg_sys::RelationIdGetRelation(oid) };

        if !index_relation.is_null() {
            if !unsafe { index_relation.as_ref() }
                .unwrap()
                .rd_amroutine
                .is_null()
                && unsafe { index_relation.as_ref().unwrap().rd_amroutine.as_ref() }
                    .unwrap()
                    .amvalidate
                    == Some(amvalidate)
            {
                unsafe {
                    pg_sys::RelationClose(index_relation);
                    return PgRelation::open(oid);
                }
            }

            unsafe {
                pg_sys::RelationClose(index_relation);
            }
        }
    }

    panic!("no zombodb index on {}", heap_relation.name())
}

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

pub fn json_to_string(key: serde_json::Value) -> Option<String> {
    match key {
        Value::Null => None,
        Value::Bool(b) => Some(if b {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s),
        _ => panic!("unsupported value type"),
    }
}
