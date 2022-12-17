#[pgx::pg_schema]
mod pg_catalog {
    use pgx::*;
    use serde::*;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum QueryStringDefaultOperator {
        and,
        or,
    }
}

#[pgx::pg_schema]
mod dsl {
    use crate::query_dsl::query_string::pg_catalog::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct QueryString<'a> {
        query: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        default_field: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        allow_leading_wildcard: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        analyze_wildcard: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        analyzer: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        auto_generate_synonyms_phrase_query: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        default_operator: Option<QueryStringDefaultOperator>,
        #[serde(skip_serializing_if = "Option::is_none")]
        enable_position_increments: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fields: Option<Array<'a, &'a str>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzziness: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzzy_max_expansions: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzzy_prefix_length: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzzy_transpositions: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        lenient: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_determinized_states: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        minimum_should_match: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        quote_analyzer: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        phrase_slop: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        quote_field_suffix: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        time_zone: Option<&'a str>,
    }

    #[pg_extern(immutable, parallel_safe)]
    fn query_string(
        query: &str,
        default_field: default!(Option<&str>, NULL),
        allow_leading_wildcard: default!(Option<bool>, NULL),
        analyze_wildcard: default!(Option<bool>, NULL),
        analyzer: default!(Option<&str>, NULL),
        auto_generate_synonyms_phrase_query: default!(Option<bool>, NULL),
        boost: default!(Option<f32>, NULL),
        default_operator: default!(Option<QueryStringDefaultOperator>, NULL),
        enable_position_increments: default!(Option<bool>, NULL),
        fields: default!(Option<Array<&str>>, NULL),
        fuzziness: default!(Option<i32>, NULL),
        fuzzy_max_expansions: default!(Option<i64>, NULL),
        fuzzy_transpositions: default!(Option<bool>, NULL),
        fuzzy_prefix_length: default!(Option<i64>, NULL),
        lenient: default!(Option<bool>, NULL),
        max_determinized_states: default!(Option<i64>, NULL),
        minimum_should_match: default!(Option<i32>, NULL),
        quote_analyzer: default!(Option<&str>, NULL),
        phrase_slop: default!(Option<i64>, NULL),
        quote_field_suffix: default!(Option<&str>, NULL),
        time_zone: default!(Option<&str>, NULL),
    ) -> ZDBQuery {
        let querystring = QueryString {
            query,
            default_field: default_field.or(Some("zdb_all")),
            allow_leading_wildcard,
            boost,
            default_operator,
            enable_position_increments,
            analyzer,
            minimum_should_match,
            quote_analyzer,
            phrase_slop,
            quote_field_suffix,
            fuzzy_max_expansions,
            fuzzy_transpositions,
            auto_generate_synonyms_phrase_query,
            analyze_wildcard,
            fields,
            fuzzy_prefix_length,
            max_determinized_states,
            time_zone,
            fuzziness,
            lenient,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "query_string":
                     querystring

            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx::pg_schema]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_test]
    fn test_query_string_with_inputs() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.query_string(
                'query input string',
                'default field',
                'true',
                'false',
                'analyzer',
                'true',
                4.5,
                'and',
                'false',
                ARRAY['doe', 'ray', 'meh'],
                3,
                10,
                'true',
                255,
                'false',
                3256,
                45008,
                'quote analyzer',
                567849,
                'quote field suffix',
                'time zoned'
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "query_string": {
                        "query": "query input string",
                        "default_field": "default field",
                        "allow_leading_wildcard": true,
                        "analyze_wildcard": false,
                        "analyzer": "analyzer",
                        "auto_generate_synonyms_phrase_query": true,
                        "boost": 4.5 as f32,
                        "default_operator": "and",
                        "enable_position_increments": false,
                        "fields": ["doe", "ray", "meh"],
                        "fuzziness": 3,
                        "fuzzy_max_expansions": 10,
                        "fuzzy_transpositions": true,
                        "fuzzy_prefix_length": 255,
                        "lenient": false,
                        "max_determinized_states": 3256,
                        "minimum_should_match": 45008,
                        "quote_analyzer": "quote analyzer",
                        "phrase_slop": 567849,
                        "quote_field_suffix" : "quote field suffix",
                        "time_zone" : "time zoned"
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_query_string_with_defaults() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.query_string(
                'query default string'
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "query_string": {
                        "query": "query default string",
                        "default_field": "zdb_all"
                    }
                }
            }
        )
    }
}
