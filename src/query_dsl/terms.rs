//! This Module is to ...
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html
//!
//! Returns documents that contain one or more exact terms in a provided field

#[pgx_macros::pg_schema]
mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    /// ```funcname
    /// terms
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn terms_str(field: &str, values: VariadicArray<&str>) -> ZDBQuery {
        make_terms_dsl(field, values)
    }

    /// ```funcname
    /// terms
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn terms_bool(field: &str, values: VariadicArray<bool>) -> ZDBQuery {
        make_terms_dsl(field, values)
    }

    /// ```funcname
    /// terms
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn terms_i16(field: &str, values: VariadicArray<i16>) -> ZDBQuery {
        make_terms_dsl(field, values)
    }

    /// ```funcname
    /// terms
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn terms_i32(field: &str, values: VariadicArray<i32>) -> ZDBQuery {
        make_terms_dsl(field, values)
    }

    /// ```funcname
    /// terms
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn terms_i64(field: &str, values: VariadicArray<i64>) -> ZDBQuery {
        make_terms_dsl(field, values)
    }

    /// ```funcname
    /// terms
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn terms_f32(field: &str, values: VariadicArray<f32>) -> ZDBQuery {
        make_terms_dsl(field, values)
    }

    /// ```funcname
    /// terms
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn terms_f64(field: &str, values: VariadicArray<f64>) -> ZDBQuery {
        make_terms_dsl(field, values)
    }

    #[inline]
    fn make_terms_dsl<T: serde::Serialize + FromDatum>(field: &str, values: Array<T>) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "terms": {
                    field: values,
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::json;

    // SELECT terms('fieldname', ARRAY[1,2,3]::integer[], 1.0);
    // SELECT terms('fieldname', ARRAY['one', 'two', 'three']::text[], 1.0);
    #[pg_test]
    fn test_terms_str() {
        let result = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.terms('fieldname', 'one'::text, 'two', 'three', 'four');",
        )
        .expect("didn't get SPI result");
        let dsl = result.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms" : {
                        "fieldname" : ["one", "two", "three", "four"],
                    }
                }

            }
        );
    }

    #[pg_test]
    fn test_terms_bool() {
        let result =
            Spi::get_one::<ZDBQuery>("SELECT dsl.terms('fieldname', true::bool,false,true,true);")
                .expect("didn't get SPI result");
        let dsl = result.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms" : {
                        "fieldname" : [true, false, true, true],
                    }
                }

            }
        );
    }

    #[pg_test]
    fn test_terms_i16() {
        let min = std::i16::MIN;
        let zero = 0_i16;
        let max = std::i16::MAX;

        let result = Spi::get_one::<ZDBQuery>(&format!(
            "SELECT dsl.terms('fieldname','{}'::smallint, {}, {});",
            min, zero, max
        ))
        .expect("didn't get SPI result");
        let dsl = result.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms" : {
                        "fieldname" : [min,zero,max],
                    }
                }

            }
        );
    }

    #[pg_test]
    fn test_terms_i32() {
        let min = std::i32::MIN;
        let zero = 0_i32;
        let max = std::i32::MAX;

        let result = Spi::get_one::<ZDBQuery>(&format!(
            "SELECT dsl.terms('fieldname', '{}'::integer, {}, {});",
            min, zero, max
        ))
        .expect("didn't get SPI result");
        let dsl = result.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms" : {
                        "fieldname" : [min,zero,max],
                    }
                }

            }
        );
    }

    #[pg_test]
    fn test_terms_i64() {
        let min = std::i64::MIN;
        let zero = 0_i64;
        let max = std::i64::MAX;

        let result = Spi::get_one::<ZDBQuery>(&format!(
            "SELECT dsl.terms('fieldname', '{}'::bigint, {}, {});",
            min, zero, max
        ))
        .expect("didn't get SPI result");
        let dsl = result.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms" : {
                        "fieldname" : [min,zero,max],
                    }
                }

            }
        );
    }

    #[pg_test]
    fn test_terms_f32() {
        let ninf = std::f32::NEG_INFINITY;
        let min = std::f32::MIN;
        let zero = 0_f32;
        let max = std::f32::MAX;
        let inf = std::f32::INFINITY;

        let result = Spi::get_one::<ZDBQuery>(&format!(
            "SELECT dsl.terms('fieldname', '{}'::real, {}, {}, {},'{}');",
            ninf, min, zero, max, inf
        ))
        .expect("didn't get SPI result");
        let dsl = result.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms" : {
                        "fieldname" : [ninf,min,zero,max,inf],
                    }
                }

            }
        );
    }

    #[pg_test]
    fn test_terms_f64() {
        let ninf = std::f64::NEG_INFINITY;
        let min = std::f64::MIN;
        let zero = 0_f64;
        let max = std::f64::MAX;
        let inf = std::f64::INFINITY;

        let result = Spi::get_one::<ZDBQuery>(&format!(
            "SELECT dsl.terms('fieldname', '{}'::double precision, {}, {},{},'{}');",
            ninf, min, zero, max, inf
        ))
        .expect("didn't get SPI result");
        let dsl = result.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "terms" : {
                        "fieldname" : [ninf,min,zero,max,inf],
                    }
                }

            }
        );
    }
}
