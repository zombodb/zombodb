#[pgx_macros::pg_schema]
pub mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct RangeStr<'a> {
        #[serde(skip_serializing_if = "Option::is_none")]
        lt: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gt: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        lte: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gte: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
    }

    #[derive(Serialize)]
    struct RangeNumber {
        #[serde(skip_serializing_if = "Option::is_none")]
        lt: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gt: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        lte: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        gte: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
    }

    /// ```funcname
    /// range
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub fn range_str(
        field: &str,
        lt: Option<default!(&str, NULL)>,
        gt: Option<default!(&str, NULL)>,
        lte: Option<default!(&str, NULL)>,
        gte: Option<default!(&str, NULL)>,
        boost: Option<default!(f32, NULL)>,
    ) -> ZDBQuery {
        let range_str = RangeStr {
            lt,
            gt,
            lte,
            gte,
            boost,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                 "range" : {
                    field : range_str
                 }
            }
        })
    }

    /// ```funcname
    /// range
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub fn range_numeric(
        field: &str,
        lt: Option<default!(i64, NULL)>,
        gt: Option<default!(i64, NULL)>,
        lte: Option<default!(i64, NULL)>,
        gte: Option<default!(i64, NULL)>,
        boost: Option<default!(f32, NULL)>,
    ) -> ZDBQuery {
        let range_numbers = RangeNumber {
            lt,
            gt,
            lte,
            gte,
            boost,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                 "range" : {
                    field : range_numbers
                 }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx_macros::pg_schema]
mod tests {
    use crate::query_dsl::range::dsl::{range_numeric, range_str};
    use pgx::*;
    use serde_json::*;

    #[pg_test]
    fn test_range_str_with_defaults() {
        let zdbquery = range_str("field", None, None, None, None, None);
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "field": {
                        }
                   }
                }
            }
        )
    }

    #[pg_test]
    fn test_range_str_without_defaults() {
        let boost = 2.9 as f32;
        let zdbquery = range_str(
            "field",
            Some("lt_value"),
            Some("gt_value"),
            Some("lte_value"),
            Some("gte_value"),
            Some(boost),
        );
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "field": {
                            "lt": "lt_value",
                            "gt": "gt_value",
                            "lte": "lte_value",
                            "gte": "gte_value",
                            "boost": boost,
                        }
                   }
                }
            }
        )
    }

    #[pg_test]
    fn test_range_number_with_defaults() {
        let zdbquery = range_numeric("field", None, None, None, None, None);
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "field": {
                        }
                   }
                }
            }
        )
    }

    #[pg_test]
    fn test_range_number_without_defaults() {
        let boost = 2.9 as f32;
        let zdbquery = range_numeric("field", Some(56), Some(67), Some(78), Some(89), Some(boost));
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "range": {
                        "field": {
                            "lt": 56,
                            "gt": 67,
                            "lte": 78,
                            "gte": 89,
                            "boost": boost,
                        }
                   }
                }
            }
        )
    }
}
