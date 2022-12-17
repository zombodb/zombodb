//! This Module is to
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-query.html
//!
//! The following search returns documents that are missing an indexed value for the user field

#[pgx::pg_schema]
mod dsl {
    use crate::zdbquery::{ZDBQuery, ZDBQueryClause};
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn field_missing(field: &str) -> ZDBQuery {
        ZDBQuery::new_with_query_clause(ZDBQueryClause::bool(
            None,
            None,
            Some(vec![ZDBQueryClause::opaque(
                json! { { "exists": { "field": field } } },
            )]),
            None,
        ))
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use crate::query_dsl::field_missing::dsl::*;
    use pgx::*;
    use serde_json::*;

    #[pg_test]
    fn test_field_missing() {
        let zdbquery = field_missing("fieldname");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                 "bool": {
                      "must_not": [
                          { "exists": {"field":  "fieldname"} }
                      ]
                 }
              }
            }
        );
    }
}
