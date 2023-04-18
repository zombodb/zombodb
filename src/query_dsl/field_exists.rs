//! This Module is to
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html
//!
//! Returns documents that contain an indexed value for a field.

#[pgrx::pg_schema]
mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgrx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn field_exists(field: &str) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
        {
            "exists": {
                "field": field
            }
        }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::query_dsl::field_exists::dsl::*;
    use pgrx::*;
    use serde_json::*;

    #[pg_test]
    fn test_field_exists() {
        let zdbquery = field_exists("fieldname");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                        {
                    "exists": {"field":  "fieldname"}
                         }
            }
        );
    }
}
