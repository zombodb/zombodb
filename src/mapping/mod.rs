use crate::utils::lookup_zdb_index_tupdesc;
use pgx::*;
use serde_json::*;
use std::collections::HashMap;

pub fn generate_mapping(indexrel: &PgBox<pg_sys::RelationData>) -> serde_json::Value {
    let tupdesc = lookup_zdb_index_tupdesc(&indexrel);
    let mut mappings = HashMap::new();

    tupdesc.iter().filter(|v| !v.is_dropped()).for_each(|att| {
        // TODO: lookup the att.oid() type and if it's an array (or a domain or something special), then
        //    do inspect it further
        mappings.insert(att.name(), lookup_type_mapping(att.oid()));
    });

    mappings.insert(
        "zdb_all",
        json! {{ "type": "text", "analyzer": "zdb_all_analyzer" }},
    );
    mappings.insert("zdb_ctid", json! {{ "type": "long" }});
    mappings.insert("zdb_cmin", json! {{ "type": "integer" }});
    mappings.insert("zdb_cmax", json! {{ "type": "integer" }});
    mappings.insert("zdb_xmin", json! {{ "type": "long" }});
    mappings.insert("zdb_xmax", json! {{ "type": "long" }});
    mappings.insert("zdb_aborted_xids", json! {{ "type": "long" }});

    serde_json::to_value(mappings).unwrap()
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

fn lookup_type_mapping(typoid: PgOid) -> serde_json::Value {
    Spi::get_one_with_args::<JsonB>(
        "SELECT definition FROM zdb.type_mappings WHERE type_name = $1;",
        vec![(
            PgOid::BuiltIn(PgBuiltInOids::OIDOID),
            typoid.clone().into_datum(),
        )],
    )
    .expect(&format!("no type mapping for {:?}", typoid))
    .0
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::mapping::generate_mapping;
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
        let index = PgBox::from_pg(pg_sys::RelationIdGetRelation(index_id));
        let mapping = generate_mapping(&index);

        assert_eq!(
            &mapping,
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
