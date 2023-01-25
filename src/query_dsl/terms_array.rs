//! This mod is for...
//! Use postgres json generator to

#[pgx::pg_schema]
mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_extern(immutable, parallel_safe)]
    fn terms_array(fieldname: &str, array: AnyArray) -> ZDBQuery {
        let array_as_json = unsafe {
            direct_function_call::<Json>(
                pg_sys::array_to_json,
                vec![array.into_datum(), false.into_datum()],
            )
        }
        .expect("anyarray conversion to json returned null");

        ZDBQuery::new_with_query_dsl(json! {
            {
                "terms": {
                    fieldname: array_as_json.0
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::json;

    #[pg_test]
    fn test_terms_array_with_integers() {
        let zdbquery =
            Spi::get_one::<ZDBQuery>("SELECT dsl.terms_array('some_field', ARRAY[1, 2, 3])")
                .expect("SPI failed")
                .expect("SPI result failed");

        assert_eq!(
            zdbquery.into_value(),
            json! {
                {
                    "terms": { "some_field": [1,2,3] }
                }
            }
        )
    }

    #[pg_test]
    fn test_terms_array_with_strings() {
        let zdbquery =
            Spi::get_one::<ZDBQuery>("SELECT dsl.terms_array('some_field', ARRAY['a', 'b', 'c'])")
                .expect("SPI failed")
                .expect("SPI result failed");

        assert_eq!(
            zdbquery.into_value(),
            json! {
                {
                    "terms": { "some_field": ["a", "b", "c"] }
                }
            }
        )
    }

    #[pg_test]
    fn test_terms_array_with_booleans() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.terms_array('some_field', ARRAY['true', 'true', 'false'])",
        )
        .expect("SPI failed")
        .expect("SPI result failed");

        assert_eq!(
            zdbquery.into_value(),
            json! {
                {
                    "terms": { "some_field": ["true", "true", "false"] }
                }
            }
        )
    }

    #[pg_test]
    fn test_terms_array_with_floats() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.terms_array('some_field', ARRAY['4.2', '5.6', '6.9'])",
        )
        .expect("SPI failed")
        .expect("SPI result failed");

        assert_eq!(
            zdbquery.into_value(),
            json! {
                {
                    "terms": { "some_field": ["4.2", "5.6", "6.9"] }
                }
            }
        )
    }
}
