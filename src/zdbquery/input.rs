use crate::zdbquery::ZDBQuery;
use pgx::*;

#[pg_extern(immutable)]
fn zdbquery_in(input: &std::ffi::CStr) -> ZDBQuery {
    ZDBQuery::from_cstr(input)
}

#[pg_extern(immutable)]
fn zdbquery_out(input: ZDBQuery) -> &'static std::ffi::CStr {
    input.into_cstr()
}

/// now that our functions have been created, create the full type
extension_sql! {r#"
CREATE TYPE pg_catalog.zdbquery (
    INTERNALLENGTH = variable,
    INPUT = zdb.zdbquery_in,
    OUTPUT = zdb.zdbquery_out,
    STORAGE = extended
);
"#}

mod tests {
    use crate::zdbquery::input::zdbquery_in;
    use pgx::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_zdbquery_in_with_query_string() {
        let zdbquery = zdbquery_in(std::ffi::CString::new("this is a test").unwrap().as_c_str());
        let json = serde_json::to_value(&zdbquery).unwrap();

        assert_eq!(
            json,
            json!( {"query_dsl":{"query_string":{"query":"this is a test"}}} )
        );
    }

    #[pg_test]
    fn test_zdbquery_in_with_query_dsl() {
        let zdbquery = zdbquery_in(
            std::ffi::CString::new(r#" {"match_all":{}} "#)
                .unwrap()
                .as_c_str(),
        );
        let json = serde_json::to_value(&zdbquery).unwrap();

        assert_eq!(json, json!( {"query_dsl":{"match_all":{}}} ));
    }

    #[pg_test]
    fn test_zdbquery_in_with_full_query() {
        let zdbquery = zdbquery_in(
            std::ffi::CString::new(
                r#" {"query_dsl":{"query_string":{"query":"this is a test"}}} "#,
            )
            .unwrap()
            .as_c_str(),
        );
        let json = serde_json::to_value(&zdbquery).unwrap();

        assert_eq!(
            json,
            json!( {"query_dsl":{"query_string":{"query":"this is a test"}}} )
        );
    }
}
