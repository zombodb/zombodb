use crate::json::builder::JsonBuilder;
use pgx::*;
use serde_json::*;
use std::collections::HashMap;

type ConversionFunc<'a> = dyn Fn(&mut JsonBuilder<'a>, &'a str, pg_sys::Datum, pg_sys::Oid);
pub struct CategorizedAttribute<'a> {
    pub attname: &'a str,
    pub dropped: bool,
    pub typoid: pg_sys::Oid,
    pub conversion_func: Box<ConversionFunc<'a>>,
}

pub fn categorize_tupdesc<'a>(
    tupdesc: &'a PgBox<pg_sys::TupleDescData>,
    mapping: &mut HashMap<&'a str, serde_json::Value>,
) -> Vec<CategorizedAttribute<'a>> {
    let mut vec = Vec::with_capacity(tupdesc.len());
    for attribute in tupdesc.iter() {
        let array_type = unsafe { pg_sys::get_element_type(attribute.oid().value()) };
        let (type_oid, is_array) = if array_type != pg_sys::InvalidOid {
            (PgOid::from(array_type), true)
        } else {
            (attribute.oid(), false)
        };

        let attname = attribute.name();
        let dropped = attribute.is_dropped();
        let typoid = attribute.oid().value();
        let conversion_func: Box<ConversionFunc> = match &type_oid {
            PgOid::BuiltIn(builtin) => match builtin {
                PgBuiltInOids::BOOLOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_bool_array(
                                name,
                                unsafe { Vec::<Option<bool>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_bool(
                                name,
                                unsafe { bool::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::INT2OID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_i16_array(
                                name,
                                unsafe { Vec::<Option<i16>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_i16(
                                name,
                                unsafe { i16::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::INT4OID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_i32_array(
                                name,
                                unsafe { Vec::<Option<i32>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_i32(
                                name,
                                unsafe { i32::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::INT8OID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_i64_array(
                                name,
                                unsafe { Vec::<Option<i64>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_i64(
                                name,
                                unsafe { i64::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::OIDOID | PgBuiltInOids::XIDOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_u32_array(
                                name,
                                unsafe { Vec::<Option<u32>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_u32(
                                name,
                                unsafe { u32::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::FLOAT4OID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_f32_array(
                                name,
                                unsafe { Vec::<Option<f32>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_f32(
                                name,
                                unsafe { f32::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::FLOAT8OID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_f64_array(
                                name,
                                unsafe { Vec::<Option<f64>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_f64(
                                name,
                                unsafe { f64::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::TIMEOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_time_array(
                                name,
                                unsafe { Vec::<Option<Time>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_time(
                                name,
                                unsafe { Time::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::TIMETZOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_time_with_time_zone_array(
                                name,
                                unsafe {
                                    Vec::<Option<TimeWithTimeZone>>::from_datum(datum, false, oid)
                                }
                                .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_time_with_time_zone(
                                name,
                                unsafe { TimeWithTimeZone::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::TIMESTAMPOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_timestamp_array(
                                name,
                                unsafe { Vec::<Option<Timestamp>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_timestamp(
                                name,
                                unsafe { Timestamp::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::TIMESTAMPTZOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_timestamp_with_timestamp_zone_array(
                                name,
                                unsafe {
                                    Vec::<Option<TimestampWithTimeZone>>::from_datum(
                                        datum, false, oid,
                                    )
                                }
                                .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_timestamp_with_timestamp_zone(
                                name,
                                unsafe { TimestampWithTimeZone::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::DATEOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_date_array(
                                name,
                                unsafe { Vec::<Option<Date>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_date(
                                name,
                                unsafe { Date::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::TEXTOID | PgBuiltInOids::VARCHAROID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_string_array(
                                name,
                                unsafe { Vec::<Option<String>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_string(
                                name,
                                unsafe { String::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::JSONOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_json_string_array(
                                name,
                                unsafe {
                                    Vec::<Option<pgx::JsonString>>::from_datum(datum, false, oid)
                                }
                                .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_json_string(
                                name,
                                unsafe { pgx::JsonString::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }
                PgBuiltInOids::JSONBOID => {
                    if is_array {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_jsonb_array(
                                name,
                                unsafe { Vec::<Option<JsonB>>::from_datum(datum, false, oid) }
                                    .unwrap(),
                            )
                        })
                    } else {
                        Box::new(|builder, name, datum, oid| {
                            builder.add_jsonb(
                                name,
                                unsafe { JsonB::from_datum(datum, false, oid) }.unwrap(),
                            )
                        })
                    }
                }

                _ => handle_unsupported_type(attribute),
            },

            PgOid::Custom(_) => handle_unsupported_type(attribute),

            PgOid::InvalidOid => panic!("{} has a type oid of InvalidOid", attribute.name()),
        };

        mapping.insert(
            attribute.name(),
            match lookup_type_mapping(&type_oid) {
                Some(json) => json,
                None => {
                    info!(
                        "Unrecognized type {:?} for {}, generating mapping as 'keyword'",
                        type_oid, attname
                    );
                    json!({"type": "keyword"})
                }
            },
        );

        vec.push(CategorizedAttribute {
            attname,
            dropped,
            typoid,
            conversion_func,
        });
    }

    vec
}

fn handle_unsupported_type<'a>(
    attribute: &pg_sys::FormData_pg_attribute,
) -> Box<ConversionFunc<'a>> {
    let mut output_func = pg_sys::InvalidOid;
    let mut is_varlena = false;

    unsafe {
        pg_sys::getTypeOutputInfo(attribute.oid().value(), &mut output_func, &mut is_varlena);
    }

    info!(
        "{} has an unsupported type: {:?}",
        attribute.name(),
        attribute.oid()
    );
    Box::new(move |builder, name, datum, _oid| {
        let result =
            unsafe { std::ffi::CStr::from_ptr(pg_sys::OidOutputFunctionCall(output_func, datum)) };
        let json = json!(result);
        builder.add_json_value(name, json);

        unsafe {
            pg_sys::pfree(result.as_ptr() as void_mut_ptr);
        }
    })
}

pub fn generate_default_mapping() -> HashMap<&'static str, serde_json::Value> {
    let mut mapping = HashMap::new();

    mapping.insert(
        "zdb_all",
        json! {{ "type": "text", "analyzer": "zdb_all_analyzer" }},
    );
    mapping.insert("zdb_ctid", json! {{ "type": "long" }});
    mapping.insert("zdb_cmin", json! {{ "type": "integer" }});
    mapping.insert("zdb_cmax", json! {{ "type": "integer" }});
    mapping.insert("zdb_xmin", json! {{ "type": "long" }});
    mapping.insert("zdb_xmax", json! {{ "type": "long" }});
    mapping.insert("zdb_aborted_xids", json! {{ "type": "long" }});

    mapping
}

pub fn lookup_analysis_thing(table_name: &str) -> Value {
    // TODO:  pg10 doesn't have "json_object_agg()" -- what do we do instead?
    match Spi::get_one::<Json>(&format!(
        "SELECT json_object_agg(name, definition) FROM zdb.{};",
        table_name
    )) {
        Some(json) => json.0,
        None => json! {{}},
    }
}

fn lookup_type_mapping(typoid: &PgOid) -> Option<serde_json::Value> {
    match Spi::get_one_with_args::<JsonB>(
        "SELECT definition FROM zdb.type_mappings WHERE type_name = $1;",
        vec![(
            PgOid::BuiltIn(PgBuiltInOids::OIDOID),
            typoid.clone().into_datum(),
        )],
    ) {
        Some(jsonb) => Some(jsonb.0),
        None => None,
    }
}

fn lookup_type_conversion(typoid: PgOid) -> Option<pg_sys::Oid> {
    Spi::get_one_with_args::<pg_sys::Oid>(
        "SELECT funcoid FROM zdb.type_conversions WHERE typeoid = $1;",
        vec![(
            PgOid::BuiltIn(PgBuiltInOids::OIDOID),
            typoid.clone().into_datum(),
        )],
    )
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::mapping::{categorize_tupdesc, generate_default_mapping};
    use pgx::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    unsafe fn test_generate_mapping() {
        Spi::run("CREATE TABLE test (id serial8, title text, name varchar);");
        Spi::run("CREATE INDEX idxtest ON test USING zombodb ((test.*)) WITH (url='http://localhost:9200/')");
        let index_id = Spi::get_one::<pg_sys::Oid>("SELECT 'idxtest'::regclass;")
            .expect("failed to get idxtest oid from SPI");
        let index: PgBox<pg_sys::RelationData> =
            PgBox::from_pg(pg_sys::RelationIdGetRelation(index_id));
        let tupdesc = PgBox::from_pg(index.rd_att);
        let mut mapping = generate_default_mapping();

        categorize_tupdesc(&tupdesc, &mut mapping);
        let mapping_json = serde_json::to_string(&mapping).unwrap();

        assert_eq!(
            &mapping_json,
            &json!({
              "id": {
                "type": "long"
              },
              "name": {
                "copy_to": "zdb_all",
                "ignore_above": 10922,
                "normalizer": "lowercase",
                "type": "keyword"
              },
              "title": {
                "analyzer": "zdb_standard",
                "copy_to": "zdb_all",
                "fielddata": true,
                "type": "text"
              },
              "zdb_aborted_xids": {
                "type": "long"
              },
              "zdb_all": {
                "analyzer": "zdb_all_analyzer",
                "type": "text"
              },
              "zdb_cmax": {
                "type": "integer"
              },
              "zdb_cmin": {
                "type": "integer"
              },
              "zdb_ctid": {
                "type": "long"
              },
              "zdb_xmax": {
                "type": "long"
              },
              "zdb_xmin": {
                "type": "long"
              }
            })
        );
    }
}
