mod pg_catalog {
    use pgx::*;
    use serde::*;
    use serde_json::Value;

    #[derive(PostgresType, Serialize, Deserialize)]
    pub struct BoolQueryPart(pub Value);
}

pub mod dsl {
    use super::pg_catalog::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;
    use std::collections::HashMap;

    #[derive(Serialize, Deserialize)]
    #[serde(rename = "bool")]
    struct Bool {
        #[serde(skip_serializing_if = "Option::is_none")]
        must: Option<Vec<Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        must_not: Option<Vec<Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        should: Option<Vec<Value>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        filter: Option<Vec<Value>>,
    }

    #[pg_extern(immutable, parallel_safe)]
    fn bool(parts: VariadicArray<BoolQueryPart>) -> ZDBQuery {
        let mut must = Vec::new();
        let mut must_not = Vec::new();
        let mut should = Vec::new();
        let mut filter = Vec::new();

        for part in parts.iter() {
            if part.is_none() {
                continue;
            }

            let part = part.unwrap();
            let mut map: HashMap<String, Vec<Value>> = serde_json::from_value(part.0).unwrap();

            must.append(map.get_mut("must").unwrap_or(&mut Vec::<Value>::new()));
            must_not.append(map.get_mut("must_not").unwrap_or(&mut Vec::<Value>::new()));
            should.append(map.get_mut("should").unwrap_or(&mut Vec::<Value>::new()));
            filter.append(map.get_mut("filter").unwrap_or(&mut Vec::<Value>::new()));

            if !map.contains_key("must")
                && !map.contains_key("must_not")
                && !map.contains_key("should")
                && !map.contains_key("filter")
            {
                panic!("invalid dsl.bool() argument: {:?}", map);
            }
        }

        let mut bool_query = Bool {
            must: None,
            must_not: None,
            should: None,
            filter: None,
        };

        if !must.is_empty() {
            bool_query.must = Some(must);
        }
        if !must_not.is_empty() {
            bool_query.must_not = Some(must_not);
        }
        if !should.is_empty() {
            bool_query.should = Some(should);
        }
        if !filter.is_empty() {
            bool_query.filter = Some(filter);
        }

        ZDBQuery::new_with_query_dsl(json!({ "bool": bool_query }))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn should(queries: VariadicArray<ZDBQuery>) -> BoolQueryPart {
        BoolQueryPart(json!({"should":
            serde_json::to_value(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in queries")
                            .query_dsl()
                            .expect("zdbquery doesn't contain query dsl")
                            .clone()
                    })
                    .collect::<Vec<Value>>(),
            )
            .unwrap(),
        }))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn must(queries: VariadicArray<ZDBQuery>) -> BoolQueryPart {
        BoolQueryPart(json!({"must":
            serde_json::to_value(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in queries")
                            .query_dsl()
                            .expect("zdbquery doesn't contain query dsl")
                            .clone()
                    })
                    .collect::<Vec<Value>>(),
            )
            .unwrap(),
        }))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn must_not(queries: VariadicArray<ZDBQuery>) -> BoolQueryPart {
        BoolQueryPart(json!({"must_not":
            serde_json::to_value(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in queries")
                            .query_dsl()
                            .expect("zdbquery doesn't contain query dsl")
                            .clone()
                    })
                    .collect::<Vec<Value>>(),
            )
            .unwrap(),
        }))
    }

    #[pg_extern(immutable, parallel_safe)]
    fn filter(queries: VariadicArray<ZDBQuery>) -> BoolQueryPart {
        BoolQueryPart(json!({"filter":
            serde_json::to_value(
                queries
                    .iter()
                    .map(|zdbquery| {
                        zdbquery
                            .expect("found NULL zdbquery in queries")
                            .query_dsl()
                            .expect("zdbquery doesn't contain query dsl")
                            .clone()
                    })
                    .collect::<Vec<Value>>(),
            )
            .unwrap(),
        }))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn binary_and(a: ZDBQuery, b: ZDBQuery) -> ZDBQuery {
        let a_dsl = a.query_dsl().unwrap().clone();
        a.set_query_dsl(Some(json! {
            {
                "bool": {
                    "must": [a_dsl, b.query_dsl()]
                }
            }
        }))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn and(queries: variadic!(Vec<Option<ZDBQuery>>)) -> ZDBQuery {
        let clauses: Vec<serde_json::Value> = queries
            .into_iter()
            .map(|zdbquery| {
                zdbquery
                    .expect("found NULL zdbquery in clauses")
                    .query_dsl()
                    .expect("zdbquery doesn't contain query dsl")
                    .clone()
            })
            .collect();
        ZDBQuery::new_with_query_dsl(json! {
            {
                "bool": {
                    "must": clauses
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn or(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        let clauses: Vec<serde_json::Value> = queries
            .iter()
            .map(|zdbquery| {
                zdbquery
                    .expect("found NULL zdbquery in clauses")
                    .query_dsl()
                    .expect("zdbquery doesn't contain query dsl")
                    .clone()
            })
            .collect();
        ZDBQuery::new_with_query_dsl(json! {
            {
                "bool": {
                    "should": clauses
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn not(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        let clauses: Vec<serde_json::Value> = queries
            .iter()
            .map(|zdbquery| {
                zdbquery
                    .expect("found NULL zdbquery in clauses")
                    .query_dsl()
                    .expect("zdbquery doesn't contain query dsl")
                    .clone()
            })
            .collect();
        ZDBQuery::new_with_query_dsl(json! {
            {
                "bool": {
                    "must_not": clauses
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn noteq(query: ZDBQuery) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "bool": {
                    "must_not": query.query_dsl()
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
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
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
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
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
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
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
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
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
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
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
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
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
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
        let dsl = zdbquery.query_dsl();

        assert_eq!(zdbquery.limit().unwrap(), 10);
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "bool": {
                        "must": [
                            {"query_string":{"query":"a"}},
                            {"query_string":{"query":"b"}}
                        ]
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_noteq() {
        let zdbquery = noteq(ZDBQuery::new_with_query_string("test_notEq"));
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "bool": {
                        "must_not": {
                            "query_string": {
                                "query": "test_notEq"
                            }
                        }
                    }
                }
            }
        );
    }
}
