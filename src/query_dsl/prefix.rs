//! This mod is to..
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html
//!
//! Returns documents that contain a specific prefix in a provided field

#[pgx::pg_schema]
mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn prefix(field: &str, value: &str) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "prefix": {
                    field: {
                        "value": value,
                        "rewrite": "constant_score"
                    }
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use crate::query_dsl::prefix::dsl::*;
    use pgx::*;
    use serde_json::json;

    #[pg_test]
    fn test_prefix() {
        let zdbquery = prefix("fieldname", "te");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "prefix": {"fieldname": {"value": "te", "rewrite": "constant_score"}}
                }
            }
        );
    }
}
