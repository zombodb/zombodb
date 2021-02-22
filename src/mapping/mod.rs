use crate::json::builder::JsonBuilder;
use crate::json::utils::escape_json;
use crate::utils::type_is_domain;
use pgx::*;
use serde_json::*;
use std::collections::HashMap;

type ConversionFunc<'a> = dyn Fn(&mut JsonBuilder<'a>, &'a str, pg_sys::Datum, pg_sys::Oid);
pub struct CategorizedAttribute<'a> {
    pub attname: &'a str,
    pub typoid: pg_sys::Oid,
    pub conversion_func: Box<ConversionFunc<'a>>,
    pub attno: usize,
}

#[allow(clippy::cognitive_complexity)]
pub fn categorize_tupdesc<'a>(
    tupdesc: &'a PgTupleDesc,
    heap_relation: &PgRelation,
    mut mapping: Option<&mut HashMap<String, serde_json::Value>>,
) -> Vec<CategorizedAttribute<'a>> {
    let mut categorized_attributes = Vec::with_capacity(tupdesc.len());
    let type_conversion_cache = lookup_type_conversions();
    let user_mappings = if mapping.is_some() {
        Some(lookup_mappings(&heap_relation))
    } else {
        None
    };

    for (attno, attribute) in tupdesc.iter().enumerate() {
        if attribute.is_dropped() {
            continue;
        }
        let attname = attribute.name();
        let mut typoid = attribute.type_oid();
        let typmod = attribute.type_mod();

        let conversion_func = type_conversion_cache.get(&typoid.value()).cloned();

        let conversion_func: Box<ConversionFunc> = if let Some(custom_converter) = conversion_func {
            // use a configured zdb.type_conversion
            Box::new(move |builder, name, datum, _oid| {
                let datum = unsafe {
                    pg_sys::OidFunctionCall2Coll(
                        custom_converter,
                        pg_sys::InvalidOid,
                        datum,
                        typmod.into_datum().unwrap(),
                    )
                };
                let json = unsafe { Json::from_datum(datum, false, typoid.value()) }
                    .expect("failed to version a type to json");
                builder.add_json_value(name, json.0);
            })
        } else {
            // use one of our built-in converters, downcasting arrays to their element type
            // as Elasticsearch doesn't require special mapping notations for arrays
            let array_type = unsafe { pg_sys::get_element_type(attribute.type_oid().value()) };

            let (base_oid, is_array) = if array_type != pg_sys::InvalidOid {
                (PgOid::from(array_type), true)
            } else {
                (attribute.type_oid(), false)
            };

            typoid = base_oid;
            match &typoid {
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
                                        Vec::<Option<TimeWithTimeZone>>::from_datum(
                                            datum, false, oid,
                                        )
                                    }
                                    .unwrap(),
                                )
                            })
                        } else {
                            Box::new(|builder, name, datum, oid| {
                                builder.add_time_with_time_zone(
                                    name,
                                    unsafe { TimeWithTimeZone::from_datum(datum, false, oid) }
                                        .unwrap(),
                                )
                            })
                        }
                    }
                    PgBuiltInOids::TIMESTAMPOID => {
                        if is_array {
                            Box::new(|builder, name, datum, oid| {
                                builder.add_timestamp_array(
                                    name,
                                    unsafe {
                                        Vec::<Option<Timestamp>>::from_datum(datum, false, oid)
                                    }
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
                                builder.add_timestamp_with_time_zone_array(
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
                                builder.add_timestamp_with_time_zone(
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
                                        Vec::<Option<pgx::JsonString>>::from_datum(
                                            datum, false, oid,
                                        )
                                    }
                                    .unwrap(),
                                )
                            })
                        } else {
                            Box::new(|builder, name, datum, oid| {
                                builder.add_json_string(
                                    name,
                                    unsafe { pgx::JsonString::from_datum(datum, false, oid) }
                                        .unwrap(),
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

                    unknown => handle_unsupported_type(is_array, unknown.value()),
                },

                PgOid::Custom(custom) => handle_unsupported_type(is_array, *custom),

                PgOid::InvalidOid => panic!("{} has a type oid of InvalidOid", attribute.name()),
            }
        };

        if mapping.is_some() {
            let definition = match user_mappings.as_ref().unwrap().get(attname).cloned() {
                Some(json) => json,
                None => match lookup_type_mapping(typoid) {
                    Some((mapping, regproc)) => match mapping {
                        Some(mapping) => mapping,
                        None => {
                            let regproc: pg_sys::Oid = regproc.unwrap();
                            unsafe {
                                let datum = pg_sys::OidFunctionCall2Coll(
                                    regproc,
                                    pg_sys::InvalidOid,
                                    typoid.into_datum().unwrap(),
                                    typmod.into_datum().unwrap(),
                                );
                                JsonB::from_datum(datum, false, pg_sys::JSONBOID).unwrap().0
                            }
                        }
                    },
                    None => {
                        match type_is_domain(typoid.value()) {
                            Some((basetypeoid, name)) => {
                                if basetypeoid == pg_sys::VARCHAROID {
                                    // varchar types are considered keywords and the domain name
                                    // becomes the "normalizer"

                                    if name == "keyword" {
                                        // unless the base type is "keyword"
                                        json!( {"type": "keyword", "ignore_above": 10922 })
                                    } else {
                                        json!( {"type": "keyword", "ignore_above": 10922, "normalizer": name })
                                    }
                                } else if basetypeoid == pg_sys::TEXTOID {
                                    // text types are mapped as "text" with the "analyzer" set to the name
                                    json!( {"type": "text", "analyzer": name })
                                } else {
                                    panic!(
                                        "Unsupported base domain type for {}: {}",
                                        name, basetypeoid
                                    );
                                }
                            }
                            None => {
                                info!(
                                    "Unrecognized type {:?} for {}, generating mapping as 'keyword'",
                                    typoid, attname
                                );
                                json!({ "type": "keyword" })
                            }
                        }
                    }
                },
            };

            mapping
                .as_mut()
                .unwrap()
                .insert(attname.to_owned(), definition);
        }

        let mut json_attname = String::with_capacity(attname.len());
        escape_json(attname, &mut json_attname);
        let attname = PgMemoryContexts::TopTransactionContext.leak_and_drop_on_delete(json_attname);
        categorized_attributes.push(CategorizedAttribute {
            attname: unsafe { attname.as_ref().unwrap() },
            typoid: typoid.value(),
            conversion_func,
            attno,
        });
    }

    categorized_attributes
}

fn handle_unsupported_type<'a>(
    is_array: bool,
    base_type_oid: pg_sys::Oid,
) -> Box<ConversionFunc<'a>> {
    let mut output_func = pg_sys::InvalidOid;
    let mut is_varlena = false;

    unsafe {
        pg_sys::getTypeOutputInfo(base_type_oid, &mut output_func, &mut is_varlena);
    }

    if is_array {
        Box::new(move |builder, name, datum, _oid| {
            let array: Array<pg_sys::Datum> =
                unsafe { Array::from_datum(datum, false, base_type_oid).unwrap() };

            // build up a vec of each element as a string
            let mut values = Vec::new();
            for e in array.iter() {
                // only serialize to json non-null array values
                if let Some(element_datum) = e {
                    let result = unsafe {
                        std::ffi::CStr::from_ptr(pg_sys::OidOutputFunctionCall(
                            output_func,
                            element_datum,
                        ))
                    };
                    let result_str = result
                        .to_str()
                        .expect("failed to convert unsupported type to a string");
                    values.push(result_str.to_string());

                    unsafe {
                        pg_sys::pfree(result.as_ptr() as void_mut_ptr);
                    }
                }
            }

            // then add that vec to our builder as a json blob
            builder.add_json_value(name, json!(values))
        })
    } else {
        Box::new(move |builder, name, datum, _oid| {
            // call the output function to convert the datum into a string
            let result = unsafe {
                std::ffi::CStr::from_ptr(pg_sys::OidOutputFunctionCall(output_func, datum))
            };
            let result_str = result
                .to_str()
                .expect("failed to convert unsupported type to a string");

            // then we add it to our builder as a json value
            builder.add_json_value(name, json!(result_str));

            unsafe {
                pg_sys::pfree(result.as_ptr() as void_mut_ptr);
            }
        })
    }
}

pub fn generate_default_mapping(heap_relation: &PgRelation) -> HashMap<String, serde_json::Value> {
    let mut mapping = HashMap::new();

    mapping.insert(
        "zdb_all".to_owned(),
        json! {{ "type": "text", "analyzer": "zdb_all_analyzer" }},
    );
    mapping.insert("zdb_ctid".to_owned(), json! {{ "type": "long" }});
    mapping.insert("zdb_cmin".to_owned(), json! {{ "type": "integer" }});
    mapping.insert("zdb_cmax".to_owned(), json! {{ "type": "integer" }});
    mapping.insert("zdb_xmin".to_owned(), json! {{ "type": "long" }});
    mapping.insert("zdb_xmax".to_owned(), json! {{ "type": "long" }});
    mapping.insert("zdb_aborted_xids".to_owned(), json! {{ "type": "long" }});

    for (field, definition) in lookup_es_only_field_mappings(heap_relation) {
        mapping.insert(field, definition);
    }

    mapping
}

pub fn lookup_es_only_field_mappings(
    heap_relation: &PgRelation,
) -> Vec<(String, serde_json::Value)> {
    let mut mappings = Vec::new();
    Spi::connect(|client| {
        let mut table = client.select("SELECT field_name, definition FROM zdb.mappings WHERE table_name = $1 and es_only = true",
                                      None,
                                      Some(
                                          vec![
                                              (PgOid::BuiltIn(PgBuiltInOids::OIDOID), heap_relation.clone().into_datum())
                                          ]
                                      )
        );

        while table.next().is_some() {
            let data = table.get_two::<String, JsonB>();
            mappings.push((
                data.0.expect("field_name is NULL"),
                data.1.expect("mapping definition is NULL").0,
            ));
        }

        Ok(Some(()))
    });
    mappings
}

pub fn lookup_mappings(heap_relation: &PgRelation) -> HashMap<String, serde_json::Value> {
    let mut mappings = HashMap::new();
    Spi::connect(|client| {
        let mut table = client.select("SELECT field_name, definition FROM zdb.mappings WHERE table_name = $1 and es_only = false",
                                      None,
                                      Some(
                                          vec![
                                              (PgOid::BuiltIn(PgBuiltInOids::OIDOID), heap_relation.clone().into_datum())
                                          ]
                                      )
        );
        while table.next().is_some() {
            let data = table.get_two::<String, JsonB>();
            mappings.insert(
                data.0.expect("fieldname was null"),
                data.1.expect("mapping definition was null").0,
            );
        }
        Ok(Some(()))
    });
    mappings
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

fn lookup_type_mapping(typoid: PgOid) -> Option<(Option<serde_json::Value>, Option<pg_sys::Oid>)> {
    let mut json = None;
    let mut regproc = None;

    Spi::connect(|client| {
        let table = client
            .select(
                "SELECT definition, funcid FROM zdb.type_mappings WHERE type_name = $1;",
                None,
                Some(vec![(
                    PgOid::BuiltIn(PgBuiltInOids::REGPROCOID),
                    typoid.clone().into_datum(),
                )]),
            )
            .first();

        let values = table.get_two::<JsonB, pg_sys::Oid>();
        json = if values.0.is_some() {
            Some(values.0.unwrap().0)
        } else {
            None
        };
        regproc = if values.1.is_some() { values.1 } else { None };

        Ok(Some(()))
    });

    if json.is_none() && regproc.is_none() {
        None
    } else {
        Some((json, regproc))
    }
}

fn lookup_type_conversions() -> HashMap<pg_sys::Oid, pg_sys::Oid> {
    let mut cache = HashMap::new();
    Spi::connect(|client| {
        let mut table = client.select(
            "SELECT typeoid, funcoid FROM zdb.type_conversions",
            None,
            None,
        );
        while table.next().is_some() {
            let (typeoid, funcoid) = table.get_two::<pg_sys::Oid, pg_sys::Oid>();
            cache.insert(typeoid.unwrap(), funcoid.unwrap());
        }

        Ok(Some(()))
    });
    cache
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::mapping::{categorize_tupdesc, generate_default_mapping};
    use crate::utils::lookup_zdb_index_tupdesc;
    use pgx::*;
    use serde_json::json;

    #[pg_test]
    #[initialize(es = true)]
    unsafe fn test_generate_mapping() {
        Spi::run("CREATE TABLE test (id serial8, title text, name varchar);");
        Spi::run("CREATE INDEX idxtest ON test USING zombodb ((test.*)) WITH (url='http://localhost:19200/')");
        let index_id = Spi::get_one::<pg_sys::Oid>("SELECT 'idxtest'::regclass;")
            .expect("failed to get idxtest oid from SPI");
        let index = PgRelation::from_pg(pg_sys::RelationIdGetRelation(index_id));
        let tupdesc = lookup_zdb_index_tupdesc(&index);
        let mut mapping = generate_default_mapping(&index.heap_relation().unwrap());

        categorize_tupdesc(
            &tupdesc,
            &index.heap_relation().unwrap(),
            Some(&mut mapping),
        );
        let mapping_json = serde_json::to_value(&mapping).unwrap();

        assert_eq!(
            &mapping_json,
            &serde_json::to_value(&json!({
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
                "index_prefixes": {},
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
            }))
            .unwrap()
        );
    }
}
