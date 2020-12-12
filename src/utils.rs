use byteorder::ReadBytesExt;
use pgx::pg_sys::AsPgCStr;
use pgx::*;
use serde::*;
use serde_json::Value;
use std::io::Read;

pub fn has_zdb_index(heap_relation: &PgRelation, current_index: &PgRelation) -> bool {
    for index in heap_relation.indicies(pg_sys::AccessShareLock as pg_sys::LOCKMODE) {
        if index.oid() != current_index.oid() && is_zdb_index(&index) {
            return true;
        }
    }

    false
}

pub fn find_zdb_index(
    any_relation: &PgRelation,
) -> std::result::Result<(PgRelation, Option<Vec<String>>), String> {
    if is_zdb_index(any_relation) {
        return Ok((any_relation.clone(), None));
    } else if is_view(any_relation) {
        #[derive(Deserialize)]
        struct ZDBComment {
            index: String,
            options: Option<Vec<String>>,
        }

        unsafe {
            let comment_ptr = pg_sys::GetComment(any_relation.oid(), pg_sys::RelationRelationId, 0);

            return if comment_ptr.is_null() {
                // no comment
                Err(format!(
                    "no ZomboDB comment on view '{}'",
                    any_relation.name()
                ))
            } else {
                match serde_json::from_str::<ZDBComment>(
                    &std::ffi::CStr::from_ptr(comment_ptr).to_string_lossy(),
                ) {
                    Ok(comment) => Ok((
                        PgRelation::open_with_name_and_share_lock(&comment.index)
                            .expect("failed to open index specified by view index"),
                        comment.options,
                    )),

                    Err(e) => Err(format!(
                        "ZomboDB comment on view '{}' is not in correct format: {}",
                        any_relation.oid(),
                        e
                    )),
                }
            };
        }
    } else {
        for index in any_relation.indicies(pg_sys::AccessShareLock as pg_sys::LOCKMODE) {
            if is_zdb_index(&index) {
                return Ok((index.to_owned(), None));
            }
        }
    }

    Err(format!(
        "Could not find a ZomboDB index for '{}'",
        any_relation.name()
    ))
}

#[inline]
pub fn is_zdb_index(index: &PgRelation) -> bool {
    #[cfg(any(feature = "pg10", feature = "pg11"))]
    let routine = index.rd_amroutine;
    #[cfg(any(feature = "pg12", feature = "pg13"))]
    let routine = index.rd_indam;

    if routine.is_null() {
        false
    } else {
        let indam = PgBox::from_pg(routine);
        indam.amvalidate == Some(crate::access_method::amvalidate)
    }
}

#[inline]
pub fn is_view(relation: &PgRelation) -> bool {
    let rel = PgBox::from_pg(relation.rd_rel);
    rel.relkind == pg_sys::RELKIND_VIEW as i8
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
        unsafe {
            list.push(pg_sys::makeString(part.as_pg_cstr()));
        }
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

pub fn is_string_field(index: &PgRelation, field: &str) -> bool {
    let field_type = lookup_es_field_type(index, field);
    field_type == "text" || field_type == "keyword"
}

pub fn is_date_field(index: &PgRelation, field: &str) -> bool {
    lookup_es_field_type(index, field) == "date"
}

pub fn is_nested_field(index: &PgRelation, field: &str) -> bool {
    lookup_es_field_type(index, field) == "nested"
}

pub fn lookup_es_field_type(index: &PgRelation, field: &str) -> String {
    let mut sql = String::new();

    sql.push_str(&format!(
        "select
        zdb.index_mapping({}::regclass)->zdb.index_name({}::regclass)->'mappings'->'properties'",
        index.oid(),
        index.oid()
    ));

    for (idx, part) in field.split('.').enumerate() {
        if idx > 0 {
            sql.push_str("->'properties'");
        }

        sql.push_str("->");
        sql.push('\'');
        sql.push_str(part);
        sql.push('\'');
    }

    sql.push_str("->>'type';");
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

#[inline(always)]
pub fn read_vlong<T: Read>(input: &mut T) -> std::io::Result<u64> {
    let mut b = input.read_u8()? as u64;
    let mut i = (b & 0x7F) as u64;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    i |= (b & 0x7F) << 7;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    i |= (b & 0x7F) << 14;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    i |= (b & 0x7F) << 21;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    i |= (b & 0x7F) << 28;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    i |= (b & 0x7F) << 35;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    i |= (b & 0x7F) << 42;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    i |= (b & 0x7F) << 49;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    i |= (b & 0x7F) << 56;
    if (b & 0x80) == 0 {
        return Ok(i);
    }
    b = input.read_u8()? as u64;
    if b != 0 && b != 1 {
        panic!("Invalid VLong");
    }
    i |= (b as u64) << 63;
    return Ok(i);
}
