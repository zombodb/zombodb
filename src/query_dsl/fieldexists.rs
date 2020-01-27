//! This Module is to
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html
//!
//! Returns documents that contain an indexed value for a field.

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub fn fieldexists(field: &str) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
        {
            "exists": {
                "field": field
            }
        }
        })
    }
}

mod tests {
    use crate::query_dsl::fieldexists::dsl::*;
    use pgx::*;
    use serde_json::*;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_field_exists() {
        let zdbquery = fieldexists("fieldname");
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                        {
                    "exists": {"field":  "fieldname"}
                         }
            }
        );
    }
}
