#[pgrx::pg_schema]
mod dsl {
    use crate::zdbquery::{ZDBQuery, ZDBQueryClause};
    use pgrx::*;

    #[pg_extern(immutable, parallel_safe)]
    pub fn constant_score(boost: f32, query: ZDBQuery) -> ZDBQuery {
        let clause = query.query_dsl();
        query.set_query_dsl(Some(ZDBQueryClause::constant_score(clause, boost)))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn boosting(
        positive_query: ZDBQuery,
        negative_query: ZDBQuery,
        negative_boost: default!(Option<f32>, NULL),
    ) -> ZDBQuery {
        ZDBQuery::new_with_query_clause(ZDBQueryClause::boosting(
            positive_query.query_dsl(),
            negative_query.query_dsl(),
            negative_boost,
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn dis_max(
        queries: Array<ZDBQuery>,
        boost: default!(Option<f32>, NULL),
        tie_breaker: default!(Option<f32>, NULL),
    ) -> ZDBQuery {
        let queries = queries
            .iter()
            .map(|zdbquery| {
                zdbquery
                    .expect("found NULL zdbquery in clauses")
                    .query_dsl()
            })
            .collect();
        ZDBQuery::new_with_query_clause(ZDBQueryClause::dis_max(queries, boost, tie_breaker))
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::query_dsl::constant_score::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgrx::*;
    use serde_json::*;

    #[pg_test]
    fn test_constant_score() {
        let boost = 1.2_f32;
        let zdbquery = constant_score(boost, ZDBQuery::new_with_query_string("test"));

        assert_eq!(
            zdbquery.into_value(),
            json! {
                {
                        "constant_score" : {
                            "filter": { "query_string": {"query": "test"}},
                            "boost" : boost,
                        }
                }
            }
        )
    }

    #[pg_test]
    fn test_boosting() {
        let boost = 0.5_f32;
        let zdbquery = boosting(
            ZDBQuery::new_with_query_string("test_pos"),
            ZDBQuery::new_with_query_string("test_neg"),
            Some(boost),
        );

        assert_eq!(
            zdbquery.into_value(),
            json! {
                {
                        "boosting" : {
                            "positive": { "query_string": {"query": "test_pos"}},
                            "negative": { "query_string": {"query": "test_neg"}},
                            "negative_boost" : boost,
                        }
                }
            }
        )
    }

    #[pg_test]
    fn test_dis_max() {
        let boost = 2.5_f32;
        let tie = 5.5_f32;
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
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
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
