mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;

    #[pg_extern(immutable, parallel_safe)]
    pub fn limit(limit: i64, query: ZDBQuery) -> ZDBQuery {
        if limit < 0 {
            panic!("limit must be positive");
        }
        query.set_limit(Some(limit as u64))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn offset(offset: i64, query: ZDBQuery) -> ZDBQuery {
        if offset < 0 {
            panic!("offset must be positive");
        }
        query.set_offset(Some(offset as u64))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn offset_limit(offset: i64, limit: i64, mut query: ZDBQuery) -> ZDBQuery {
        query = self::offset(offset, query);
        query.set_limit(Some(limit as u64))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn min_score(min_score: f64, query: ZDBQuery) -> ZDBQuery {
        query.set_min_score(Some(min_score))
    }

    #[pg_extern(immutable, parallel_safe)]
    pub fn row_estimate(row_estimate: f64, query: ZDBQuery) -> ZDBQuery {
        query.set_row_estimate(Some(row_estimate as u64))
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::query_dsl::limit::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_test]
    fn test_limit() {
        let zdbquery = limit(100, ZDBQuery::new_with_query_string("test"));

        assert_eq!(
            zdbquery.into_value(),
            json! {
                { "limit": 100, "query_dsl": { "query_string": { "query": "test" } } }
            }
        )
    }

    #[pg_test]
    fn test_offset() {
        let zdbquery = offset(10, ZDBQuery::new_with_query_string("test"));

        assert_eq!(
            zdbquery.into_value(),
            json! {
                { "offset": 10, "query_dsl": { "query_string": { "query": "test" } } }
            }
        )
    }

    #[pg_test]
    fn test_offset_limit() {
        let zdbquery = offset_limit(100, 50, ZDBQuery::new_with_query_string("test"));

        assert_eq!(
            zdbquery.into_value(),
            json! {
                { "offset": 100,"limit": 50, "query_dsl": { "query_string": { "query": "test" } } }
            }
        )
    }

    #[pg_test]
    fn test_min_score() {
        let zdbquery = min_score(100.0, ZDBQuery::new_with_query_string("test"));

        assert_eq!(
            zdbquery.into_value(),
            json! {
                { "min_score": 100.0, "query_dsl": { "query_string": { "query": "test" } } }
            }
        )
    }

    #[pg_test]
    fn test_row_estimate() {
        let zdbquery = row_estimate(200.0, ZDBQuery::new_with_query_string("test"));

        assert_eq!(
            zdbquery.into_value(),
            json! {
                { "query_dsl": { "query_string": { "query": "test" } } , "row_estimate": 200 }
            }
        )
    }
}
