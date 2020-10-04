use pgx::*;
use serde_json::Value;

pub fn has_zdb_index(heap_relation: &PgRelation, current_index: &PgRelation) -> bool {
    for index in heap_relation.indicies(pg_sys::AccessShareLock as pg_sys::LOCKMODE) {
        if index.oid() != current_index.oid() && is_zdb_index(&index) {
            return true;
        }
    }

    false
}

pub fn find_zdb_index(heap_relation: &PgRelation) -> PgRelation {
    for index in heap_relation.indicies(pg_sys::AccessShareLock as pg_sys::LOCKMODE) {
        if is_zdb_index(&index) {
            return index.to_owned();
        }
    }

    panic!("no zombodb index on {}", heap_relation.name())
}

pub fn is_zdb_index(index: &PgRelation) -> bool {
    #[cfg(any(feature = "pg10", feature = "pg11"))]
    let routine = index.rd_amroutine;
    #[cfg(feature = "pg12")]
    let routine = index.rd_indam;

    if routine.is_null() {
        false
    } else {
        let indam = PgBox::from_pg(routine);
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
    PgMemoryContexts::TopTransactionContext.switch_to(|_| unsafe {
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

pub fn get_search_analyzer(index: &PgRelation, field: &str) -> String {
    Spi::get_one_with_args(
        "select zdb.get_search_analyzer($1, $2);",
        vec![
            (PgBuiltInOids::OIDOID.oid(), index.oid().into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
        ],
    )
    .expect("search analyzer was null")
}

#[allow(dead_code)]
pub fn get_index_analyzer(index: &PgRelation, field: &str) -> String {
    Spi::get_one_with_args(
        "select zdb.get_index_analyzer($1, $2);",
        vec![
            (PgBuiltInOids::OIDOID.oid(), index.oid().into_datum()),
            (PgBuiltInOids::TEXTOID.oid(), field.into_datum()),
        ],
    )
    .expect("search analyzer was null")
}

pub fn get_null_copy_to_fields(index: &PgRelation) -> Vec<String> {
    let mut fields = Vec::new();

    Spi::connect(|client| {
        let mut results = client.select(
            "select * from zdb.get_null_copy_to_fields($1);",
            None,
            Some(vec![(
                PgBuiltInOids::OIDOID.oid(),
                index.oid().into_datum(),
            )]),
        );

        while results.next().is_some() {
            fields.push(results.get_one().expect("field name was null"))
        }

        Ok(Some(()))
    });

    fields
}

pub fn is_nested_field(index: &PgRelation, field: &str) -> bool {
    let mut sql = String::new();

    sql.push_str(&format!(
        "select
        zdb.index_mapping({}::regclass)->zdb.index_name({}::regclass)->'mappings'->'properties'",
        index.oid(),
        index.oid()
    ));

    for (idx, part) in field.split('.').enumerate() {
        // 'data'->'properties'->'not_nested_obj');

        if idx > 0 {
            sql.push_str("->'properties'");
        }

        sql.push_str("->");
        sql.push('\'');
        sql.push_str(part);
        sql.push('\'');
    }

    sql.push_str("->>'type' is not distinct from 'nested';");
    Spi::get_one(&sql).unwrap_or_default()
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

pub fn type_is_domain(typoid: pg_sys::Oid) -> Option<(pg_sys::Oid, String)> {
    let (is_domain, base_type, name) = Spi::get_three_with_args::<bool, pg_sys::Oid, String>(
        "SELECT typtype = 'd', typbasetype, typname::text FROM pg_type WHERE oid = $1",
        vec![(PgBuiltInOids::OIDOID.oid(), typoid.into_datum())],
    );

    if is_domain.unwrap_or(false) {
        Some((base_type.unwrap(), name.unwrap()))
    } else {
        None
    }
}
