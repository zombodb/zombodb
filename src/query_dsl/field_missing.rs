//! This Module is to
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html
//!
//! The following search returns documents that are missing an indexed value for the user field

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn field_missing(field: &str) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
        {
            "bool": {
                "must_not": {
                    "exists": {
                    "field": field
                   }
               }
            }
        }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::query_dsl::field_missing::dsl::*;
    use pgx::*;
    use serde_json::*;

    #[pg_test]
    fn test_field_missing() {
        let zdbquery = field_missing("fieldname");
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                 "bool": {
                      "must_not": {
                          "exists": {"field":  "fieldname"}
                      }
                 }
              }
            }
        );
    }
}
