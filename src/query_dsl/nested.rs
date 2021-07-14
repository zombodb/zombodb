#[pgx_macros::pg_schema]
pub mod pg_catalog {
    use pgx::*;
    use serde::*;

    #[allow(non_camel_case_types)]
    #[derive(Clone, Copy, Debug, PostgresEnum, Serialize, Deserialize)]
    pub enum ScoreMode {
        avg,
        sum,
        min,
        max,
        none,
    }
}

#[pgx_macros::pg_schema]
mod dsl {
    use crate::query_dsl::nested::pg_catalog::ScoreMode;
    use crate::zdbquery::{ZDBQuery, ZDBQueryClause};
    use pgx::*;

    #[pg_extern(immutable, parallel_safe)]
    fn nested(
        path: String,
        query: ZDBQuery,
        score_mode: default!(ScoreMode, "'avg'"),
        ignore_unmapped: Option<default!(bool, "NULL")>,
    ) -> ZDBQuery {
        ZDBQuery::new_with_query_clause(ZDBQueryClause::nested(
            path,
            query.query_dsl(),
            score_mode,
            ignore_unmapped,
        ))
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
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
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
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
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
