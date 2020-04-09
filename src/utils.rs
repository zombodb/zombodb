use pgx::*;
use serde_json::Value;

pub fn find_zdb_index(heap_relation: &PgRelation) -> PgRelation {
    let index_list = PgList::<pg_sys::Oid>::from_pg(unsafe {
        pg_sys::RelationGetIndexList(heap_relation.as_ptr())
    });

    let zdb_amhandler_oid = lookup_zdb_amhandler_oid();
    for i in 0..index_list.len() {
        let oid = index_list.get_oid(i).unwrap();
        let index_relation: *mut pg_sys::RelationData =
            unsafe { pg_sys::RelationIdGetRelation(oid) };

        if !index_relation.is_null() {
            let amhandler_oid = unsafe { index_relation.as_ref().unwrap().rd_amhandler };
            if amhandler_oid == zdb_amhandler_oid {
                unsafe {
                    return PgRelation::from_pg_owned(index_relation);
                }
            }

            unsafe {
                pg_sys::RelationClose(index_relation);
            }
        }
    }

    panic!("no zombodb index on {}", heap_relation.name())
}

fn lookup_zdb_amhandler_oid() -> pg_sys::Oid {
    match Spi::get_one::<pg_sys::Oid>("SELECT 'zdb.amhandler'::regproc::oid") {
        Some(oid) => oid,
        None => panic!("no zombodb pg_am entry.  Is ZomboDB installed?"),
    }
    // NB:  lookup_function() doesn't seem to work when running in a parallel worker
    // lookup_function(vec!["zdb", "amhandler"], Some(vec![pg_sys::INTERNALOID]))
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
