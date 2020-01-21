//!This Module is to
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
//! Returns documents that contain an exact term in a provided field

use crate::zdbquery::ZDBQuery;
use pgx::*;
use serde_json::*;
use std::iter::FromIterator;

/// ```funcname
/// term
/// ```
#[pg_extern(immutable)]
fn term_str(field: &str, value: &str, boost: Option<f32>) -> ZDBQuery {
    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: {
                    "value": value,
                    "boost": boost.unwrap_or(1.0)
                }
            }
        }
    })
}

/// ```funcname
/// term
/// ```
#[pg_extern(immutable)]
fn term_bool(field: &str, value: bool, boost: Option<f32>) -> ZDBQuery {
    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: {
                    "value": value,
                    "boost": boost.unwrap_or(1.0)
                }
            }
        }
    })
}

/// ```funcname
/// term
/// ```
#[pg_extern(immutable)]
fn term_i8(field: &str, value: i8, boost: Option<f32>) -> ZDBQuery {
    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: {
                    "value": value,
                    "boost": boost.unwrap_or(1.0)
                }
            }
        }
    })
}

/// ```funcname
/// term
/// ```
#[pg_extern(immutable)]
fn term_i16(field: &str, value: i16, boost: Option<f32>) -> ZDBQuery {
    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: {
                    "value": value,
                    "boost": boost.unwrap_or(1.0)
                }
            }
        }
    })
}

/// ```funcname
/// term
/// ```
#[pg_extern(immutable)]
fn term_i32(field: &str, value: i32, boost: Option<f32>) -> ZDBQuery {
    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: {
                    "value": value,
                    "boost": boost.unwrap_or(1.0)
                }
            }
        }
    })
}

/// ```funcname
/// term
/// ```
#[pg_extern(immutable)]
fn term_i64(field: &str, value: i64, boost: Option<f32>) -> ZDBQuery {
    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: {
                    "value": value,
                    "boost": boost.unwrap_or(1.0)
                }
            }
        }
    })
}

/// ```funcname
/// term
/// ```
#[pg_extern(immutable)]
fn term_f32(field: &str, value: f32, boost: Option<f32>) -> ZDBQuery {
    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: {
                    "value": value,
                    "boost": boost.unwrap_or(1.0)
                }
            }
        }
    })
}

/// ```funcname
/// term
/// ```
#[pg_extern(immutable)]
fn term_f64(field: &str, value: f64, boost: Option<f32>) -> ZDBQuery {
    ZDBQuery::new_with_query_dsl(json! {
        {
            "term": {
                field: {
                    "value": value,
                    "boost": boost.unwrap_or(1.0)
                }
            }
        }
    })
}

mod tests {
    use crate::query_dsl::term::*;
    use pgx::*;
    use pgx_tests::*;
    use serde_json::json;
    use std::f32::{INFINITY, NAN, NEG_INFINITY};

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_term_str() {
        let zdbquery = term_str("fieldname", "test value", Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term": {"fieldname": { "value": "test value", "boost": 42.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_term_str_with_default_boost() {
        let zdbquery = term_str("fieldname", "test value", None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term": {"fieldname": { "value": "test value", "boost": 1.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_term_bool_true() {
        let zdbquery = term_bool("fieldname", true, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname": {"value": true,"boost":42.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_term_bool_false() {
        let zdbquery = term_bool("fieldname", false, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname": {"value": false,"boost":42.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_term_bool_true_with_default_boost() {
        let zdbquery = term_bool("fieldname", true, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname": {"value": true,"boost":1.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_term_bool_false_with_default_boost() {
        let zdbquery = term_bool("fieldname", false, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname": {"value": false,"boost":1.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_term_positive_i8() {
        let zdbquery = term_i8("fieldname", 127, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": 127,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_negative_i8() {
        let zdbquery = term_i8("fieldname", -100, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": -100,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_i8_with_default_boost() {
        let zdbquery = term_i8("fieldname", 127, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": 127,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_positive_i16() {
        let zdbquery = term_i16("fieldname", 32767, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": 32767,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_negative_i16() {
        let zdbquery = term_i16("fieldname", -32700, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": -32700,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_i16_with_default_boost() {
        let zdbquery = term_i16("fieldname", 32767, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": 32767,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_positive_i32() {
        let zdbquery = term_i32("fieldname", 2147483647, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": 2147483647,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_negative_i32() {
        let zdbquery = term_i32("fieldname", -2147483648, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": -2147483648,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_i32_with_default_boost() {
        let zdbquery = term_i32("fieldname", 2147483647, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": 2147483647,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_positive_i64() {
        let zdbquery = term_i64("fieldname", i64::max_value(), Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": i64::max_value(),"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_negative_i64() {
        let zdbquery = term_i64("fieldname", i64::min_value(), Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": i64::min_value(),"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_i64_with_default_boost() {
        let value = 9223372036854775000;
        let zdbquery = term_i64("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_positive_f32() {
        let value = 4.6;
        let zdbquery = term_f32("fieldname", value, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_negative_f32() {
        let value = -4.8;
        let zdbquery = term_f32("fieldname", value, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_f32_with_default_boost() {
        let value = 5.6;
        let zdbquery = term_f32("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_f32_with_positive_infinity() {
        let value = INFINITY;
        let zdbquery = term_f32("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_f32_with_negative_infinity() {
        let value = NEG_INFINITY;
        let zdbquery = term_f32("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_f32_with_nan() {
        let value = NAN;
        let zdbquery = term_f32("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_positive_f64() {
        let value = 5.6;
        let zdbquery = term_f64("fieldname", value, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_negative_f64() {
        let value = -5.6;
        let zdbquery = term_f64("fieldname", value, Some(42.0));
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":42.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_f64_with_default_boost() {
        let value = 5.6;
        let zdbquery = term_f64("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }
    #[pg_test]
    fn test_term_f64_with_infinity() {
        let value = std::f64::INFINITY;
        let zdbquery = term_f64("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_f64_with_negative_infinity() {
        let value = std::f64::NEG_INFINITY;
        let zdbquery = term_f64("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }

    #[pg_test]
    fn test_term_f64_with_nan() {
        let value = std::f64::NAN;
        let zdbquery = term_f64("fieldname", value, None);
        let dsl = zdbquery.query_dsl();

        assert!(dsl.is_some());
        assert_eq!(
            dsl.unwrap(),
            &json! {
                {
                    "term":{"fieldname":{"value": value,"boost":1.0}}
                }
            }
        )
    }
}
