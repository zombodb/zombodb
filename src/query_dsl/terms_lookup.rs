//! This module is to...
//!https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
//!
//!Terms lookup fetches the field values of an existing document.
//! Elasticsearch then uses those values as search terms. This can be helpful when searching for a large set of terms.

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use std::collections::HashMap;

    #[derive(Serialize)]
    struct TermsLookup<'a> {
        index: &'a str,
        id: &'a str,
        path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        routing: Option<&'a str>,
    }

    #[derive(Serialize)]
    struct Terms<'a> {
        terms: HashMap<&'a str, TermsLookup<'a>>,
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn terms_lookup(
        field: &str,
        index: &str,
        id: &str,
        path: &str,
        routing: Option<&str>,
    ) -> ZDBQuery {
        let mut terms = HashMap::new();
        terms.insert(
            field,
            TermsLookup {
                index,
                id,
                path,
                routing,
            },
        );

        ZDBQuery::new_with_query_dsl(serde_json::to_value(Terms { terms }).unwrap())
    }
}

mod tests {
    use crate::query_dsl::terms_lookup::dsl::*;
    use pgx::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_terms_lookup_with_routing() {
        let zdbquery = terms_lookup(
            "fieldname",
            "test value",
            "42.0",
            "path.test.example.foo",
            Some("routing.info"),
        );
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
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
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "terms": {"fieldname": { "index": "test value", "id": "42.0", "path": "path.test.example.foo" }}
                }
            }
        );
    }
}
