use pgx::*;
use serde_json::Value;

pub fn find_zdb_index(heap_relation: &PgRelation) -> PgRelation {
    for oid in heap_relation.indicies().iter_oid() {
        let index_relation =
            PgRelation::with_lock(oid, pg_sys::AccessShareLock as pg_sys::LOCKMODE);

        if is_zdb_index(&index_relation) {
            return index_relation.to_owned();
        }
    }

    panic!("no zombodb index on {}", heap_relation.name())
}

pub fn is_zdb_index(index: &PgRelation) -> bool {
    if index.rd_indam.is_null() {
        false
    } else {
        let indam = PgBox::from_pg(index.rd_indam);
        indam.amvalidate == Some(crate::access_method::amvalidate)
    }
}

pub fn lookup_zdb_extension_oid() -> pg_sys::Oid {
    match Spi::get_one::<pg_sys::Oid>("SELECT oid FROM pg_extension WHERE extname = 'zombodb';") {
        Some(oid) => oid,
        None => panic!("no zombodb pg_extension entry.  Is ZomboDB installed?"),
    }
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

pub fn lookup_function(
    name_parts: Vec<&str>,
    arg_oids: Option<Vec<pg_sys::Oid>>,
) -> Option<pg_sys::Oid> {
    let mut list = PgList::new();
    for part in name_parts {
        list.push(
            PgNodeFactory::makeString(PgMemoryContexts::CurrentMemoryContext, part).into_pg(),
        );
    }

    let (num_args, args_ptr) = match arg_oids {
        Some(oids) => (oids.len(), oids.as_ptr()),
        None => (0, vec![].as_ptr()),
    };

    let func_oid =
        unsafe { pg_sys::LookupFuncName(list.as_ptr(), num_args as i32, args_ptr, true) };

    if func_oid == pg_sys::InvalidOid {
        None
    } else {
        Some(func_oid)
    }
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
