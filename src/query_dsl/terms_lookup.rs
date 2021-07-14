//! This module is to...
//!https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
//!
//!Terms lookup fetches the field values of an existing document.
//! Elasticsearch then uses those values as search terms. This can be helpful when searching for a large set of terms.

#[pgx_macros::pg_schema]
pub mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct TermsLookup<'a> {
        index: &'a str,
        id: &'a str,
        path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        routing: Option<&'a str>,
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn terms_lookup(
        field: &str,
        index: &str,
        id: &str,
        path: &str,
        routing: Option<default!(&str, "NULL")>,
    ) -> ZDBQuery {
        let terms_lookup_object = TermsLookup {
            index,
            id,
            path,
            routing,
        };

        ZDBQuery::new_with_query_dsl(json! {
            {
                "terms": {
                    field: terms_lookup_object
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::query_dsl::terms_lookup::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::json;

    #[pg_test]
    fn test_terms_lookup_with_routing() {
        let zdbquery = terms_lookup(
            "fieldname",
            "test value",
            "42.0",
            "path.test.example.foo",
            Some("routing.info"),
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms": {"fieldname": { "index": "test value", "id": "42.0", "path": "path.test.example.foo", "routing": "routing.info"  }}
                }
            }
        );
    }

    #[pg_test]
    fn test_terms_lookup_without_routing() {
        let zdbquery = terms_lookup(
            "fieldname",
            "test value",
            "42.0",
            "path.test.example.foo",
            None,
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms": {"fieldname": { "index": "test value", "id": "42.0", "path": "path.test.example.foo" }}
                }
            }
        );
    }

    #[pg_test]
    fn test_terms_lookup_with_default_routing() {
        let zdbquery =
            Spi::get_one::<ZDBQuery>("SELECT dsl.terms_lookup('field', 'index', 'id', 'path')")
                .expect("failed to get SPI result");

        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms": {"field": { "index": "index", "id": "id", "path": "path" }}
                }
            }
        );
    }
}
