mod pg_catalog {
    use pgx::*;
    use serde::*;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum ScoreMode {
        avg,
        sum,
        min,
        max,
        none,
    }
}

mod dsl {
    use crate::query_dsl::nested::pg_catalog::ScoreMode;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct Nested<'a> {
        path: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        query: Option<serde_json::Value>,
        score_mode: ScoreMode,
        #[serde(skip_serializing_if = "Option::is_none")]
        ignore_unmapped: Option<bool>,
    }

    #[pg_extern(immutable, parallel_safe)]
    fn nested(
        path: &str,
        query: ZDBQuery,
        score_mode: default!(ScoreMode, "avg"),
        ignore_unmapped: Option<default!(bool, NULL)>,
    ) -> ZDBQuery {
        let nest = Nested {
            path,
            query: query.query_dsl().cloned(),
            score_mode,
            ignore_unmapped,
        };
        ZDBQuery::new_with_query_dsl(json! {
        {
            "nested": nest,
        }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_test]
    fn test_nested_with_default() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.nested(
                        'path_test',
                        'test'
                    )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "nested":
                        {
                            "path": "path_test",
                            "query":{ "query_string":{ "query": "test" }},
                            "score_mode": "avg",
                        }
                }
            }
        );
    }

    #[pg_test]
    fn test_nested_without_default() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.nested(
                        'path_test',
                        'test',
                        'sum',
                        'false'
                    )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "nested":
                        {
                            "path": "path_test",
                            "query":{ "query_string":{ "query": "test" }},
                            "score_mode": "sum",
                            "ignore_unmapped": false,
                        }
                }
            }
        );
    }
}
