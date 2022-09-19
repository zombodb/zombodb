#[pgx_macros::pg_schema]
mod pg_catalog {
    use crate::zdbquery::ZDBQueryClause;
    use pgx::*;
    use serde::*;

    #[derive(Debug, PostgresType, Serialize, Deserialize)]
    pub struct BoolQueryPart(pub ZDBQueryClause);
}

#[pgx_macros::pg_schema]
pub mod dsl {
    use super::pg_catalog::*;
    use crate::zdbquery::{ZDBQuery, ZDBQueryClause};
    use pgx::*;

    #[pg_extern(immutable, parallel_safe)]
    fn bool(parts: VariadicArray<BoolQueryPart>) -> ZDBQuery {
        let mut m = Vec::new();
        let mut s = Vec::new();
        let mut n = Vec::new();
        let mut f = Vec::new();

        for part in parts.iter() {
            if let Some(part) = part {
                if let Some(bool) = part.0.into_bool() {
                    m.append(&mut bool.must.unwrap_or_default());
                    s.append(&mut bool.should.unwrap_or_default());
                    n.append(&mut bool.must_not.unwrap_or_default());
                    f.append(&mut bool.filter.unwrap_or_default());
                } else {
                    panic!("invalid dsl.bool() argument");
                }
            }
        }

        ZDBQuery::new_with_query_clause(ZDBQueryClause::bool(
            if m.is_empty() { None } else { Some(m) },
            if s.is_empty() { None } else { Some(s) },
            if n.is_empty() { None } else { Some(n) },
            if f.is_empty() { None } else { Some(f) },
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn should(queries: VariadicArray<ZDBQuery>) -> BoolQueryPart {
        BoolQueryPart(ZDBQueryClause::bool(
            None,
            Some(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in queries")
                            .query_dsl()
                    })
                    .collect(),
            ),
            None,
            None,
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn must(queries: VariadicArray<ZDBQuery>) -> BoolQueryPart {
        BoolQueryPart(ZDBQueryClause::bool(
            Some(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in queries")
                            .query_dsl()
                    })
                    .collect(),
            ),
            None,
            None,
            None,
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn must_not(queries: VariadicArray<ZDBQuery>) -> BoolQueryPart {
        BoolQueryPart(ZDBQueryClause::bool(
            None,
            None,
            Some(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in queries")
                            .query_dsl()
                    })
                    .collect(),
            ),
            None,
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn filter(queries: VariadicArray<ZDBQuery>) -> BoolQueryPart {
        BoolQueryPart(ZDBQueryClause::bool(
            None,
            None,
            None,
            Some(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in queries")
                            .query_dsl()
                    })
                    .collect(),
            ),
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn binary_and(a: ZDBQuery, b: ZDBQuery) -> ZDBQuery {
        let a_dsl = a.query_dsl();
        let b_dsl = b.query_dsl();
        a.set_query_dsl(Some(ZDBQueryClause::bool(
            Some(vec![a_dsl, b_dsl]),
            None,
            None,
            None,
        )))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn and(queries: Vec<Option<ZDBQuery>>) -> ZDBQuery {
        ZDBQuery::new_with_query_clause(ZDBQueryClause::bool(
            Some(
                queries
                    .into_iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in clauses")
                            .query_dsl()
                    })
                    .collect(),
            ),
            None,
            None,
            None,
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn or(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        ZDBQuery::new_with_query_clause(ZDBQueryClause::bool(
            None,
            Some(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in clauses")
                            .query_dsl()
                    })
                    .collect(),
            ),
            None,
            None,
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn not(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        ZDBQuery::new_with_query_clause(ZDBQueryClause::bool(
            None,
            None,
            Some(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in clauses")
                            .query_dsl()
                    })
                    .collect(),
            ),
            None,
        ))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn noteq(query: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_clause(ZDBQueryClause::bool(
            None,
            None,
            Some(vec![query.query_dsl()]),
            None,
        ))
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx_macros::pg_schema]
mod tests {
    use crate::query_dsl::bool::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::json;

    #[pg_test]
    fn test_bool_all_part_types() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.bool(dsl.must('q1', 'q2', 'q3'),dsl.must_not('q4','q5'),dsl.should('q6','q7'),dsl.filter('q8','q9'))")
            .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "bool" : {
                        "must" :[
                            {
                                "query_string": {
                                    "query": "q1"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q2"
                                }
                            },
                            {
                                "query_string": {
                                    "query": "q3"
                                }
                            }
                        ],
                        "must_not" :[
                        {
                                "query_string": {
                                    "query": "q4"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q5"
                                }
                            }
                        ],
                        "should" :[
                        {
                                "query_string": {
                                    "query": "q6"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q7"
                                }
                            }
                        ],
                        "filter" :[
                        {
                                "query_string": {
                                    "query": "q8"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q9"
                                }
                            }
                        ],
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_bool_without_filter() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.bool(dsl.must('q1', 'q2', 'q3'),dsl.must_not('q4','q5'),dsl.should('q6','q7'))")
            .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "bool" : {
                        "must" :[
                            {
                                "query_string": {
                                    "query": "q1"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q2"
                                }
                            },
                            {
                                "query_string": {
                                    "query": "q3"
                                }
                            }
                        ],
                        "must_not" :[
                        {
                                "query_string": {
                                    "query": "q4"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q5"
                                }
                            }
                        ],
                        "should" :[
                        {
                                "query_string": {
                                    "query": "q6"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q7"
                                }
                            }
                        ],
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_bool_with_must_twice() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.bool(dsl.must('q1', 'q2'),dsl.must_not('q4','q5'),dsl.should('q6','q7'),dsl.must('q3'))")
            .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "bool" : {
                        "must" :[
                            {
                                "query_string": {
                                    "query": "q1"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q2"
                                }
                            },
                            {
                                "query_string": {
                                    "query": "q3"
                                }
                            }
                        ],
                        "must_not" :[
                        {
                                "query_string": {
                                    "query": "q4"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q5"
                                }
                            }
                        ],
                        "should" :[
                        {
                                "query_string": {
                                    "query": "q6"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q7"
                                }
                            }
                        ],
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_and() {
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.and('q1', 'q2')")
            .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "bool" : {
                        "must" :[
                            {
                                "query_string": {
                                    "query": "q1"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q2"
                                }
                            },
                        ],
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_or() {
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.or('q1', 'q2')")
            .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "bool" : {
                        "should" :[
                            {
                                "query_string": {
                                    "query": "q1"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q2"
                                }
                            },
                        ],
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_not() {
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.not('q1', 'q2')")
            .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "bool" : {
                        "must_not" :[
                            {
                                "query_string": {
                                    "query": "q1"
                                }
                            },
                            {
                                "query_string": {
                                     "query": "q2"
                                }
                            },
                        ],
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_binary_and() {
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.binary_and(dsl.limit(10, 'a'), 'b');")
            .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "limit": 10,
                    "query_dsl": {
                        "bool": {
                            "must": [
                                {"query_string":{"query":"a"}},
                                {"query_string":{"query":"b"}}
                            ]
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_noteq() {
        let zdbquery = noteq(ZDBQuery::new_with_query_string("test_notEq"));
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "bool": {
                        "must_not": [
                            {
                                "query_string": {
                                    "query": "test_notEq"
                                }
                            }
                        ]
                    }
                }
            }
        );
    }
}
