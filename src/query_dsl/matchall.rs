//! This mod is to
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html
//!
//!The most simple query, which matches all documents, giving them all a _score of 1.0

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn matchall(boost: default!(f32, 1.0)) -> ZDBQuery {
        make_matchall_dsl(boost)
    }

    #[inline]
    fn make_matchall_dsl(boost: f32) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
             {
                  "match_all": { "boost" : boost }
             }
        })
    }
}

mod tests {
    use crate::query_dsl::matchall::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use pgx_tests::*;
    use serde_json::json;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_matchall_with_boost() {
        let zdbquery = matchall(42.0);
        let dls = zdbquery.query_dsl();

        assert!(dls.is_some());
        assert_eq!(
            dls.unwrap(),
            &json! {
                {
                    "match_all": { "boost": 42.0}
                }
            }
        );
    }

    #[pg_test]
    fn test_matchall_with_default() {
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.matchall();")
            .expect("didn't get SPI return value");
        let dls = zdbquery.query_dsl();

        assert!(dls.is_some());
        assert_eq!(
            dls.unwrap(),
            &json! {
                {
                    "match_all": { "boost": 1.0}
                }
            }
        );
    }
}
