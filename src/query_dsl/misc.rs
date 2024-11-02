//! This mod is to...
//! https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
//!
//!Returns documents that contain terms matching a wildcard pattern.

#[pgrx::pg_schema]
mod pg_catalog {
    use pgrx::*;
    use serde::*;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum RegexFlags {
        all,
        complement,
        interval,
        intersection,
        anystring,
    }
}

#[pgrx::pg_schema]
mod dsl {
    use crate::query_dsl::misc::pg_catalog::RegexFlags;
    use crate::zdbquery::ZDBQuery;
    use pgrx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct Script<'a> {
        source: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        params: Option<Json>,
        lang: &'a str,
    }

    #[derive(Serialize)]
    struct Regexp<'a> {
        regexp: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        flags: Option<Array<'a, RegexFlags>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_determinized_states: Option<i32>,
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(super) fn wildcard(field: &str, value: &str, boost: default!(f32, 1.0)) -> ZDBQuery {
        ZDBQuery::new_with_query_dsl(json! {
            {
                "wildcard": {
                    field: {
                        "value": value,
                        "boost": boost
                    }
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn regexp(
        field: &str,
        regexp: &str,
        boost: default!(Option<f32>, NULL),
        flags: default!(Option<Array<RegexFlags>>, NULL),
        max_determinized_states: default!(Option<i32>, NULL),
    ) -> ZDBQuery {
        let regexp = Regexp {
            regexp,
            boost,
            flags,
            max_determinized_states,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "regexp": {
                    field: regexp
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    pub(crate) fn script(
        source: &str,
        params: default!(Option<Json>, NULL),
        lang: default!(&str, "'painless'"),
    ) -> ZDBQuery {
        let script = Script {
            source,
            params,
            lang,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "script": {
                    "script": script
                }
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::query_dsl::misc::dsl::*;
    use crate::zdbquery::ZDBQuery;
    use pgrx::*;
    use serde_json::*;

    #[pg_test]
    fn test_wildcard_with_boost() {
        let zdbquery = wildcard("fieldname", "t*t", 42.0);
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "wildcard": {"fieldname": {"value": "t*t", "boost": 42.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_wildcard_with_default() {
        let zdbquery = Spi::get_one::<ZDBQuery>("SELECT dsl.wildcard('fieldname', 't*t');")
            .expect("SPI failed")
            .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "wildcard": {"fieldname": {"value": "t*t", "boost": 1.0}}
                }
            }
        );
    }

    #[pg_test]
    fn test_regexp_with_default() {
        let zdbquery = regexp("this_is_the_fieldname", "regexp", None, None, None);
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "regexp": {
                         "this_is_the_fieldname": {
                            "regexp": "regexp"
                         }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_regexp_without_default() {
        let boost = 2.0_f32;
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.regexp(
                'regexp_field',
                'regular expression',
                2.0,
                ARRAY [
                    'interval',
                    'intersection',
                    'anystring'
                ]::RegexFlags[],
                32
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "regexp": {
                        "regexp_field": {
                            "regexp": "regular expression",
                            "boost": boost,
                            "max_determinized_states": 32,
                            "flags": ["interval", "intersection", "anystring" ],
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_script_with_default() {
        let zdbquery = script("script_source", None, "painless");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "script" : {
                        "script" : {
                            "source" : "script_source",
                            "lang": "painless",
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_script_without_default() {
        let json_example = Spi::get_one::<Json>(r#"  SELECT '{"json": "json_value"}'::json;  "#)
            .expect("SPI failed")
            .unwrap();
        let zdbquery = script("script_source", Some(json_example), "totally_a_lang");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "script" : {
                        "script" : {
                            "source" : "script_source",
                            "lang": "totally_a_lang",
                            "params": {
                                "json": "json_value",
                            }
                        }
                    }
                }
            }
        )
    }
}
