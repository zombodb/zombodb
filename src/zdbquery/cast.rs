use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde_json::Value;

#[pg_extern(immutable)]
fn zdbquery_from_text(input: &str) -> ZDBQuery {
    ZDBQuery::input(input).expect("failed to convert text into a zdbquery")
}

#[pg_extern(immutable)]
fn zdbquery_from_json(input: Json) -> ZDBQuery {
    serde_json::from_value(input.0).expect("failed to deserialize json into a zdbquery")
}

#[pg_extern(immutable)]
fn zdbquery_from_jsonb(input: JsonB) -> ZDBQuery {
    serde_json::from_value(input.0).expect("failed to deserialize jsonb into a zdbquery")
}

#[pg_extern(immutable)]
fn zdbquery_to_json(input: ZDBQuery) -> Json {
    Json(serde_json::to_value(input).expect("failed to serialize zdbquery to json"))
}

#[pg_extern(immutable)]
fn zdbquery_to_jsonb(input: ZDBQuery) -> JsonB {
    JsonB(serde_json::to_value(input).expect("failed to serialize zdbquery to jsonb"))
}

extension_sql! {r#"
CREATE CAST (text AS zdbquery) WITH FUNCTION zdbquery_from_text(text) AS IMPLICIT;
CREATE CAST (json AS zdbquery) WITH FUNCTION zdbquery_from_json(json) AS IMPLICIT;
CREATE CAST (jsonb AS zdbquery) WITH FUNCTION zdbquery_from_jsonb(jsonb) AS IMPLICIT;
CREATE CAST (zdbquery AS json) WITH FUNCTION zdbquery_to_json(zdbquery) AS IMPLICIT;
CREATE CAST (zdbquery AS jsonb) WITH FUNCTION zdbquery_to_jsonb(zdbquery) AS IMPLICIT;
"#}

mod tests {
    use crate::zdbquery::cast::zdbquery_from_text;
    use pgx::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_zdbquery_from_random_text() {
        let zdbquery = zdbquery_from_text("test 2");
        let json = serde_json::to_value(&zdbquery).unwrap();

        assert_eq!(
            json,
            json!( {"query_dsl":{"query_string":{"query":"test 2"}}} )
        );
    }

    #[pg_test]
    fn test_zdbquery_from_full_json_text() {
        let zdbquery =
            zdbquery_from_text(r#"   {"query_dsl":{"query_string":{"query":"test 2"}}}   "#);
        let json = serde_json::to_value(&zdbquery).unwrap();

        assert_eq!(
            json,
            json!( {"query_dsl":{"query_string":{"query":"test 2"}}} )
        );
    }
}
