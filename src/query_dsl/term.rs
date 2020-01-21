//! This Module is to
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html
//!
//! Returns documents that contain an exact term in a provided field

mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;
    use std::iter::FromIterator;

    /// ```funcname
    /// term
    /// ```
    #[pg_extern(immutable)]
    pub(super) fn term_str(field: &str, value: &str, boost: default!(f32, 1.0)) -> ZDBQuery {
        make_term_dsl(field, value, boost)
    }

    /// ```funcname
    /// term
    /// ```
    #[pg_extern(immutable)]
    pub(super) fn term_bool(field: &str, value: bool, boost: default!(f32, 1.0)) -> ZDBQuery {
        make_term_dsl(field, value, boost)
    }

    /// ```funcname
    /// term
    /// ```
    #[pg_extern(immutable)]
    pub(super) fn term_i16(field: &str, value: i16, boost: default!(f32, 1.0)) -> ZDBQuery {
        make_term_dsl(field, value, boost)
    }

    /// ```funcname
    /// term
    /// ```
    #[pg_extern(immutable)]
    pub(super) fn term_i32(field: &str, value: i32, boost: default!(f32, 1.0)) -> ZDBQuery {
        make_term_dsl(field, value, boost)
    }

    /// ```funcname
    /// term
    /// ```
    #[pg_extern(immutable)]
    pub(super) fn term_i64(field: &str, value: i64, boost: default!(f32, 1.0)) -> ZDBQuery {
        make_term_dsl(field, value, boost)
    }

    /// ```funcname
    /// term
    /// ```
    #[pg_extern(immutable)]
    pub(super) fn term_f32(field: &str, value: f32, boost: default!(f32, 1.0)) -> ZDBQuery {
        make_term_dsl(field, value, boost)
    }

    /// ```funcname
    /// term
    /// ```
    #[pg_extern(immutable)]
    pub(super) fn term_f64(field: &str, value: f64, boost: default!(f32, 1.0)) -> ZDBQuery {
        make_term_dsl(field, value, boost)
    }

    #[inline]
    fn make_term_dsl<T: serde::Serialize>(field: &str, value: T, boost: f32) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "term": {
                    field: {
                        "value": value,
                        "boost": boost
                    }
                }
            }
        })
    }
}

mod tests {
    use crate::query_dsl::term::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use pgx_tests::*;
    use serde_json::json;
    use std::f32::{INFINITY, NAN, NEG_INFINITY};

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_term_str() {
        let zdbquery = term_str("fieldname", "test value", 42.0);
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
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', 'test value');")
            .expect("didn't get SPI return value");
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
        let zdbquery = term_bool("fieldname", true, 42.0);
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
        let zdbquery = term_bool("fieldname", false, 42.0);
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
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', true);")
            .expect("didn't get SPI return value");
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
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', false);")
            .expect("didn't get SPI return value");
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
    fn test_term_positive_i16() {
        let zdbquery = term_i16("fieldname", 32767, 42.0);
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
        let zdbquery = term_i16("fieldname", -32700, 42.0);
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
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', 32767);")
            .expect("didn't get SPI return value");
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
        let zdbquery = term_i32("fieldname", 2147483647, 42.0);
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
        let zdbquery = term_i32("fieldname", -2147483648, 42.0);
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
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', 2147483647);")
            .expect("didn't get SPI return value");
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
        let zdbquery = term_i64("fieldname", i64::max_value(), 42.0);
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
        let zdbquery = term_i64("fieldname", i64::min_value(), 42.0);
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
        let value = 9223372036854775000 as i64;
        let zdbquery =
            Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', 9223372036854775000);")
                .expect("didn't get SPI return value");
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
        let zdbquery = term_f32("fieldname", value, 42.0);
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
        let zdbquery = term_f32("fieldname", value, 42.0);
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
        let zdbquery =
            Spi::get_one::<ZDBQuery>(&format!("SELECT dsl.term('fieldname', {});", value))
                .expect("didn't get SPI return value");
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
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', 'infinity'::real);")
            .expect("didn't get SPI return value");
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
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', '-infinity'::real);")
            .expect("didn't get SPI return value");
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
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', 'nan'::real);")
            .expect("didn't get SPI return value");
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
        let zdbquery = term_f64("fieldname", value, 42.0);
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
        let zdbquery = term_f64("fieldname", value, 42.0);
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
        let zdbquery = Spi::get_one::<ZDBQuery>(&format!(
            "SELECT dsl.term('fieldname', {}::double precision);",
            value
        ))
        .expect("didn't get SPI return value");
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
        let zdbquery =
            Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', 'infinity'::double precision);")
                .expect("didn't get SPI return value");
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
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.term('fieldname', '-infinity'::double precision);",
        )
        .expect("didn't get SPI return value");
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
        let zdbquery =
            Spi::get_one::<ZDBQuery>("SELECT dsl.term('fieldname', 'nan'::double precision);")
                .expect("didn't get SPI return value");
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
