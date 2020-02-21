mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct DisMax {
        queries: Vec<Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tie_breaker: Option<f32>,
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn constant_score(query: ZDBQuery, boost: default!(f32, NULL)) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "constant_score": {
                    "filter":
                        query.query_dsl().expect("'constanst score' zdbquery doesn't contain query dsl"),
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
                    "positive" : positive_query.query_dsl().expect("'positive' zdbquery doesn't contain query dsl"),
                    "negative" : negative_query.query_dsl().expect("'negative' zdbquery doesn't contain query dsl"),
                    "negative_boost" : negative_boost,
                },
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn dis_max(
        queries: Array<ZDBQuery>,
        boost: Option<default!(f32, NULL)>,
        tie_breaker: Option<default!(f32, NULL)>,
    ) -> ZDBQuery {
        let queries: Vec<serde_json::Value> = queries
            .iter()
            .map(|zdbquery| {
                zdbquery
                    .expect("found NULL zdbquery in clauses")
                    .query_dsl()
                    .expect("zdbquery doesn't contain query dsl")
                    .clone()
            })
            .collect();
        let dismax = DisMax {
            queries,
            boost,
            tie_breaker,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "dis_max": dismax,
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
                            "filter": { "query_string": {"query": "test"}},
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
                            "positive": { "query_string": {"query": "test_pos"}},
                            "negative": { "query_string": {"query": "test_neg"}},
                            "negative_boost" : boost,
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_dis_max() {
        let boost = 2.5 as f32;
        let tie = 5.5 as f32;
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.dis_max(
            ARRAY[
                 dsl.term('term_field1', 'term_value1'),
                 dsl.term('term_field2', 'term_value2'),
                 dsl.term('term_field3', 'term_value3')
            ],
            '2.5',
            '5.5'
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "dis_max" : {
                        "queries": [
                            {"term": {"term_field1": { "value": "term_value1"}}},
                            {"term": {"term_field2": { "value": "term_value2"}}},
                            {"term": {"term_field3": { "value": "term_value3"}}},
                        ],
                        "boost" : boost,
                        "tie_breaker": tie,
                    }
                }
            }
        )
    }
}
