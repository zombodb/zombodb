//! This mod is to...
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
//!
//!Returns documents that contain terms matching a wildcard pattern.

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;
    use std::iter::FromIterator;

    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn wildcard(field: &str, value: &str, boost: default!(f32, 1.0)) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "wildcard": {
                    field: {
                        "value": value,
                        "boost": boost
                    }
                }
            }
        })
    }
}

mod tests {
    use crate::query_dsl::wildcard::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use pgx_tests::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_wildcard_with_boost() {
        let zdbquery = wildcard("fieldname", "t*t", 42.0);
        let dls = zdbquery.query_dsl();

        assert!(dls.is_some());
        assert_eq!(
            dls.unwrap(),
            &json! {
                {
                    "wildcard": {"fieldname": {"value": "t*t", "boost": 42.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_wildcard_with_default() {
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.wildcard('fieldname', 't*t');")
            .expect("didn't get SPI return value");
        let dls = zdbquery.query_dsl();

        assert!(dls.is_some());
        assert_eq!(
            dls.unwrap(),
            &json! {
                {
                    "wildcard": {"fieldname": {"value": "t*t", "boost": 1.0}}
                }
            }
        );
    }
}
