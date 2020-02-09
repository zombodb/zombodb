mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    fn bool(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        let queries_vec: Vec<serde_json::Value> = queries
            .iter()
            .map(|zdbquery| {
                zdbquery
                    .expect("found NULL zdbquery in queries")
                    .query_dsl()
                    .expect("zdbquery doesn't contain query dsl")
                    .clone()
            })
            .collect();

        ZDBQuery::new_with_query_dsl(json! {
            {
                "bool" : queries_vec,
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn should(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        bool(queries)
    }

    #[pg_extern(immutable, parallel_safe)]
    fn must(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        bool(queries)
    }

    #[pg_extern(immutable, parallel_safe)]
    fn must_not(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        bool(queries)
    }

    #[pg_extern(immutable, parallel_safe)]
    fn filter(queries: VariadicArray<ZDBQuery>) -> ZDBQuery {
        bool(queries)
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::json;

    #[pg_test]
    fn test_bool_without_nulls() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.bool(
                dsl.term('term_field1', 'term_value1'), 
                dsl.term('term_field2', 'term_value2')
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "bool" : [
                        {
                            "term" :{
                                "term_field1" : {
                                    "value": "term_value1",
                                    "boost": 1.0,
                                }
                            }
                        },
                            {
                            "term" :{
                                "term_field2" : {
                                    "value"  : "term_value2",
                                    "boost": 1.0,
                                }
                            }
                        }
                    ],
                }
            }
        );
    }

    #[pg_test(error = "found NULL zdbquery in queries")]
    fn test_bool_with_null() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.bool(
                dsl.term('field1', 'value1'), 
                dsl.term('field2', 'value2'), 
                null /* this causes the error */
            )",
        )
        .expect("failed to get SPI result");
        zdbquery.limit(); //using this to prevent an warning that zdbquery is not used
    }

    #[pg_test]
    fn test_must() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.must(
                dsl.term('term_field1', 'term_value1'),
                dsl.term('term_field2', 'term_value2')
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "bool" : [
                        {
                            "term" :{
                                "term_field1" : {
                                    "value": "term_value1",
                                    "boost": 1.0,
                                }
                            }
                        },
                            {
                            "term" :{
                                "term_field2" : {
                                    "value"  : "term_value2",
                                    "boost": 1.0,
                                }
                            }
                        }
                    ],
                }
            }
        );
    }

    #[pg_test]
    fn test_must_not() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.must_not(
                dsl.term('term_field1', 'term_value1'),
                dsl.term('term_field2', 'term_value2')
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "bool" : [
                        {
                            "term" :{
                                "term_field1" : {
                                    "value": "term_value1",
                                    "boost": 1.0,
                                }
                            }
                        },
                            {
                            "term" :{
                                "term_field2" : {
                                    "value"  : "term_value2",
                                    "boost": 1.0,
                                }
                            }
                        }
                    ],
                }
            }
        );
    }

    #[pg_test]
    fn test_should() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.should(
                dsl.term('term_field1', 'term_value1'),
                dsl.term('term_field2', 'term_value2')
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "bool" : [
                        {
                            "term" :{
                                "term_field1" : {
                                    "value": "term_value1",
                                    "boost": 1.0,
                                }
                            }
                        },
                            {
                            "term" :{
                                "term_field2" : {
                                    "value"  : "term_value2",
                                    "boost": 1.0,
                                }
                            }
                        }
                    ],
                }
            }
        );
    }

    #[pg_test]
    fn test_filter() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.filter(
                dsl.term('term_field1', 'term_value1'),
                dsl.term('term_field2', 'term_value2')
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.query_dsl();

        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "bool" : [
                        {
                            "term" :{
                                "term_field1" : {
                                    "value": "term_value1",
                                    "boost": 1.0,
                                }
                            }
                        },
                            {
                            "term" :{
                                "term_field2" : {
                                    "value"  : "term_value2",
                                    "boost": 1.0,
                                }
                            }
                        }
                    ],
                }
            }
        );
    }
}
