use crate::elasticsearch::{Elasticsearch, ElasticsearchBulkRequest};
use crate::json::builder::JsonBuilder;
use crate::json::json_string::JsonString;
use pgx::*;
use std::ops::DerefMut;

struct Attribute<'a> {
    dropped: bool,
    name: &'a str,
    typoid: PgOid,
}

struct BuildState<'a> {
    ntuples: usize,
    bulk: ElasticsearchBulkRequest,
    tupdesc: &'a PgBox<pg_sys::TupleDescData>,
    attributes: Vec<Attribute<'a>>,
}

impl<'a> BuildState<'a> {
    fn new(url: &'a str, index_name: &'a str, tupdesc: &'a PgBox<pg_sys::TupleDescData>) -> Self {
        let mut attributes = Vec::new();
        for i in 0..tupdesc.natts {
            let attr = tupdesc_get_attr(&tupdesc, i as usize);
            attributes.push(Attribute {
                dropped: attr.attisdropped,
                name: name_data_to_str(&attr.attname),
                typoid: PgOid::from(attr.atttypid),
            });
        }

        BuildState {
            ntuples: 0,
            bulk: Elasticsearch::new(url, index_name).start_bulk(),
            tupdesc: &tupdesc,
            attributes,
        }
    }
}

#[pg_guard]
pub extern "C" fn ambuild(
    heap_relation: pg_sys::Relation,
    index_relation: pg_sys::Relation,
    index_info: *mut pg_sys::IndexInfo,
) -> *mut pg_sys::IndexBuildResult {
    let mut result = PgBox::<pg_sys::IndexBuildResult>::alloc0();
    let result_mut = result.deref_mut();

    let tupdesc = PgBox::from_pg(PgBox::from_pg(index_relation).rd_att);
    // lookup the tuple descriptor for the rowtype we're *indexing*, rather than
    // using the tuple descriptor for the index definition itself
    let tupdesc = PgBox::from_pg(unsafe {
        pg_sys::lookup_rowtype_tupdesc(
            tupdesc_get_typoid(&tupdesc, 1),
            tupdesc_get_typmod(&tupdesc, 1),
        )
    });

    let mut state = BuildState::new("http://localhost:9200/", "test_index", &tupdesc);

    unsafe {
        pg_sys::IndexBuildHeapScan(
            heap_relation,
            index_relation,
            index_info,
            Some(build_callback),
            &mut state,
        );
    }
    if tupdesc.tdrefcount >= 0 {
        unsafe {
            pg_sys::DecrTupleDescRefCount(tupdesc.as_ptr());
        }
    }

    info!("Waiting to finish");
    match state.bulk.wait_for_completion() {
        Ok(cnt) => info!("indexed {} tuples", cnt),
        Err(e) => panic!("{:?}", e),
    }

    info!("ntuples={}", state.ntuples);
    result_mut.heap_tuples = state.ntuples as f64;
    result_mut.index_tuples = state.ntuples as f64;

    result.into_pg()
}

#[pg_guard]
pub extern "C" fn ambuildempty(_index_relation: pg_sys::Relation) {}

#[pg_guard]
pub extern "C" fn aminsert(
    _index_relation: pg_sys::Relation,
    _values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _heap_tid: pg_sys::ItemPointer,
    _heap_relation: pg_sys::Relation,
    _check_unique: pg_sys::IndexUniqueCheck,
    _index_info: *mut pg_sys::IndexInfo,
) -> bool {
    info!("aminsert");
    false
}

unsafe extern "C" fn build_callback(
    _index: pg_sys::Relation,
    htup: pg_sys::HeapTuple,
    values: *mut pg_sys::Datum,
    _isnull: *mut bool,
    _tuple_is_alive: bool,
    state: *mut std::os::raw::c_void,
) {
    check_for_interrupts!();

    let htup = PgBox::from_pg(htup);
    let mut state = PgBox::from_pg(state as *mut BuildState);
    let values = std::slice::from_raw_parts(values, 1);
    let row_datum = values[0];
    let (attnames, values) = row_to_json(row_datum, &state);
    let ctid = htup.t_self.clone();

    state
        .bulk
        .insert(ctid, 0, 0, 0, 0, JsonBuilder::new(attnames, values))
        .expect("Unable to send tuple for insert");
    state.ntuples += 1;

    if state.ntuples % 10000 == 0 {
        info!("cnt={}", state.ntuples);
    }
}

unsafe fn row_to_json<'a>(
    row: pg_sys::Datum,
    state: &PgBox<BuildState>,
) -> (Vec<String>, Vec<Box<dyn JsonString>>) {
    let columns = deconstruct_row_type(state.tupdesc, row);

    let mut attnames = Vec::with_capacity(state.tupdesc.natts as usize);
    let mut json = Vec::<Box<dyn JsonString>>::with_capacity(state.tupdesc.natts as usize);
    for (i, attr) in state.attributes.iter().enumerate() {
        if attr.dropped {
            continue;
        }

        let datum = columns.get(i).unwrap();
        match datum {
            None => {
                // we don't bother to encode null values
            }
            Some(datum) => {
                attnames.push(attr.name.to_owned());
                match &attr.typoid {
                    PgOid::InvalidOid => panic!("Found InvalidOid for attname='{}'", attr.name),
                    PgOid::Custom(oid) => {
                        // TODO:  what to do here?
                        unimplemented!("Found custom oid={}", oid);
                    }
                    PgOid::BuiltIn(oid) => match oid {
                        PgBuiltInOids::TEXTOID => {
                            json.push(Box::new(
                                String::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::VARCHAROID => {
                            json.push(Box::new(
                                String::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::BOOLOID => {
                            json.push(Box::new(
                                bool::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::INT2OID => {
                            json.push(Box::new(
                                i16::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::INT4OID => {
                            json.push(Box::new(
                                i32::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::INT8OID => {
                            json.push(Box::new(
                                i64::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::OIDOID => {
                            json.push(Box::new(
                                u32::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::XIDOID => {
                            json.push(Box::new(
                                u32::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::FLOAT4OID => {
                            json.push(Box::new(
                                f32::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::FLOAT8OID => {
                            json.push(Box::new(
                                f64::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::JSONOID => {
                            json.push(Box::new(
                                // JSON types are simple varlena* strings, so we avoid parsing the JSON
                                // and just treat it as a string
                                pgx::JsonString::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        PgBuiltInOids::JSONBOID => {
                            json.push(Box::new(
                                JsonB::from_datum(datum, false, attr.typoid.value()).unwrap(),
                            ));
                        }
                        PgBuiltInOids::BOOLARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<bool>>::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        PgBuiltInOids::INT2ARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<i16>>::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        PgBuiltInOids::INT4ARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<i32>>::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        PgBuiltInOids::INT8ARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<i64>>::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        PgBuiltInOids::TEXTARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<String>>::from_datum(
                                    datum,
                                    false,
                                    attr.typoid.value(),
                                )
                                .unwrap(),
                            ));
                        }
                        PgBuiltInOids::VARCHARARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<String>>::from_datum(
                                    datum,
                                    false,
                                    attr.typoid.value(),
                                )
                                .unwrap(),
                            ));
                        }
                        PgBuiltInOids::OIDARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<u32>>::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        PgBuiltInOids::FLOAT4ARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<f32>>::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        PgBuiltInOids::FLOAT8ARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<f64>>::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        PgBuiltInOids::JSONARRAYOID => {
                            json.push(Box::new(
                                // JSON types are simple varlena* strings, so we avoid parsing the JSON
                                // and just treat it as a string
                                Vec::<Option<pgx::JsonString>>::from_datum(
                                    datum,
                                    false,
                                    attr.typoid.value(),
                                )
                                .unwrap(),
                            ));
                        }
                        PgBuiltInOids::JSONBARRAYOID => {
                            json.push(Box::new(
                                Vec::<Option<JsonB>>::from_datum(datum, false, attr.typoid.value())
                                    .unwrap(),
                            ));
                        }
                        _ => {
                            //                            let value_as_json = direct_function_call::<pgx::JsonString>(
                            //                                pg_sys::to_json,
                            //                                vec![Some(datum)],
                            //                            )
                            //                            .expect("detected null while converting unknown type to json");
                            //                            json.push(Box::new(value_as_json));

                            json.push(Box::new(format!(
                                "UNSUPPORTED TYPE: {}",
                                attr.typoid.value()
                            )));
                        }
                    },
                }
            }
        }
    }

    (attnames, json)
}
