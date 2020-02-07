mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    pub fn constant_score(query: ZDBQuery, boost: default!(f32, NULL)) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "constant_score": {
                    "filter":
                        query,
                    "boost": boost,
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn boosting(
        positive_query: ZDBQuery,
        negative_query: ZDBQuery,
        negative_boost: default!(f32, NULL),
    ) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "boosting" : {
                    "positive" : positive_query,
                    "negative" : negative_query,
                    "negative_boost" : negative_boost,
                },
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::query_dsl::constant_score::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_test]
    fn test_constant_score() {
        let boost = 1.2 as f32;
        let zdbquery = constant_score(ZDBQuery::new_with_query_string("test"), boost);

        assert_eq!(
            zdbquery.into_value(),
            json! {
                {
                    "query_dsl" : {
                        "constant_score" : {
                            "filter": { "query_dsl": {"query_string": {"query": "test"}}},

                            "boost" : boost,
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_boosting() {
        let boost = 0.5 as f32;
        let zdbquery = boosting(
            ZDBQuery::new_with_query_string("test_pos"),
            ZDBQuery::new_with_query_string("test_neg"),
            boost,
        );

        assert_eq!(
            zdbquery.into_value(),
            json! {
                {
                    "query_dsl" : {
                        "boosting" : {
                            "positive": { "query_dsl": {"query_string": {"query": "test_pos"}}},
                            "negative": { "query_dsl": {"query_string": {"query": "test_neg"}}},
                            "negative_boost" : boost,
                        }
                    }
                }
            }
        )
    }
}
