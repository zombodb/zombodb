//! This mod is to
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html
//!
//! This is the inverse of the match_all query, which matches no documents

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn matchnone() -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
             {
               "match_none": { }
             }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::query_dsl::match_none::dsl::*;
    use pgx::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_matchnone() {
        let zdbquery = matchnone();
        let dls = zdbquery.query_dsl();

        assert!(dls.is_some());
        assert_eq!(
            dls.unwrap(),
            &json! {
                {
                    "match_none": { }
                }
            }
        );
    }
}
