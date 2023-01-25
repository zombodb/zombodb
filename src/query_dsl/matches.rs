#[pgx::pg_schema]
mod pg_catalog {
    use pgx::*;
    use serde::*;

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum ZeroTermsQuery {
        none,
        all,
    }

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum Operator {
        and,
        or,
    }

    #[allow(non_camel_case_types)]
    #[derive(PostgresEnum, Serialize)]
    pub enum MatchType {
        best_fields,
        most_fields,
        cross_fields,
        phrase,
        phrase_prefix,
    }
}

#[pgx::pg_schema]
mod dsl {
    use crate::query_dsl::matches::pg_catalog::*;
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct Match_<'a> {
        query: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        analyzer: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        minimum_should_match: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        lenient: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzziness: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzzy_rewrite: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzzy_transpositions: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        prefix_length: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cutoff_frequency: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        auto_generate_synonyms_phrase_query: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        zero_terms_query: Option<ZeroTermsQuery>,
        #[serde(skip_serializing_if = "Option::is_none")]
        operator: Option<Operator>,
    }

    #[derive(Serialize)]
    struct MultiMatched<'a> {
        query: &'a str,
        fields: Array<'a, &'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        analyzer: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        minimum_should_match: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        lenient: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzziness: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzzy_rewrite: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fuzzy_transpositions: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        prefix_length: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cutoff_frequency: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        auto_generate_synonyms_phrase_query: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        zero_terms_query: Option<ZeroTermsQuery>,
        #[serde(skip_serializing_if = "Option::is_none")]
        operator: Option<Operator>,
        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(rename = "type")]
        match_type: Option<MatchType>,
    }

    #[derive(Serialize)]
    struct MatchPhrase<'a> {
        query: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        slop: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        analyzer: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        zero_terms_query: Option<ZeroTermsQuery>,
    }

    #[derive(Serialize)]
    struct MatchPhrasePrefix<'a> {
        query: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        slop: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        analyzer: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_expansions: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        zero_terms_query: Option<ZeroTermsQuery>,
    }

    #[pg_extern(immutable, parallel_safe, name = "match")]
    fn match_wrapper(
        field: &str,
        query: &str,
        boost: default!(Option<f32>, NULL),
        analyzer: default!(Option<&str>, NULL),
        minimum_should_match: default!(Option<i32>, NULL),
        lenient: default!(Option<bool>, NULL),
        fuzziness: default!(Option<i32>, NULL),
        fuzzy_rewrite: default!(Option<&str>, NULL),
        fuzzy_transpositions: default!(Option<bool>, NULL),
        prefix_length: default!(Option<i32>, NULL),
        cutoff_frequency: default!(Option<f32>, NULL),
        auto_generate_synonyms_phrase_query: default!(Option<bool>, NULL),
        zero_terms_query: default!(Option<ZeroTermsQuery>, NULL),
        operator: default!(Option<Operator>, NULL),
    ) -> ZDBQuery {
        let match_ = Match_ {
            query,
            boost,
            analyzer,
            minimum_should_match,
            lenient,
            fuzziness,
            fuzzy_rewrite,
            fuzzy_transpositions,
            prefix_length,
            cutoff_frequency,
            auto_generate_synonyms_phrase_query,
            zero_terms_query,
            operator,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "match": {
                    field: match_
                }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn multi_match(
        fields: Array<&str>,
        query: &str,
        boost: default!(Option<f32>, NULL),
        analyzer: default!(Option<&str>, NULL),
        minimum_should_match: default!(Option<i32>, NULL),
        lenient: default!(Option<bool>, NULL),
        fuzziness: default!(Option<i32>, NULL),
        fuzzy_rewrite: default!(Option<&str>, NULL),
        fuzzy_transpositions: default!(Option<bool>, NULL),
        prefix_length: default!(Option<i32>, NULL),
        cutoff_frequency: default!(Option<f32>, NULL),
        auto_generate_synonyms_phrase_query: default!(Option<bool>, NULL),
        zero_terms_query: default!(Option<ZeroTermsQuery>, NULL),
        operator: default!(Option<Operator>, NULL),
        match_type: default!(Option<MatchType>, NULL),
    ) -> ZDBQuery {
        let multimatch = MultiMatched {
            query,
            fields,
            boost,
            analyzer,
            minimum_should_match,
            lenient,
            fuzziness,
            fuzzy_rewrite,
            fuzzy_transpositions,
            prefix_length,
            cutoff_frequency,
            auto_generate_synonyms_phrase_query,
            zero_terms_query,
            operator,
            match_type,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "multi_match": multimatch
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn match_phrase(
        field: &str,
        query: &str,
        boost: default!(Option<f32>, NULL),
        slop: default!(Option<i32>, NULL),
        analyzer: default!(Option<&str>, NULL),
        zero_terms_query: default!(Option<ZeroTermsQuery>, NULL),
    ) -> ZDBQuery {
        let match_phrase = MatchPhrase {
            query,
            boost,
            slop,
            analyzer,
            zero_terms_query,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                 "match_phrase" : {
                    field : match_phrase,
                 }
            }
        })
    }

    #[pg_extern(immutable, parallel_safe)]
    fn phrase(
        field: &str,
        query: &str,
        boost: default!(Option<f32>, NULL),
        slop: default!(Option<i32>, NULL),
        analyzer: default!(Option<&str>, NULL),
        zero_terms_query: default!(Option<ZeroTermsQuery>, NULL),
    ) -> ZDBQuery {
        match_phrase(field, query, boost, slop, analyzer, zero_terms_query)
    }

    #[pg_extern(immutable, parallel_safe)]
    fn match_phrase_prefix(
        field: &str,
        query: &str,
        boost: default!(Option<f32>, NULL),
        slop: default!(Option<i32>, NULL),
        analyzer: default!(Option<&str>, NULL),
        maxexpansion: default!(Option<i32>, NULL),
        zero_terms_query: default!(Option<ZeroTermsQuery>, NULL),
    ) -> ZDBQuery {
        let match_phrase_prefix = MatchPhrasePrefix {
            query,
            boost,
            slop,
            analyzer,
            max_expansions: maxexpansion,
            zero_terms_query,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                 "match_phrase_prefix" : {
                    field : match_phrase_prefix,
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
    use serde_json::*;

    #[pg_test]
    fn test_match_without_defaults() {
        let boost = 2.0 as f32;
        let cutoff = 3.9 as f32;
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.match(
                'match_field',
                'match_query',
                2.0,
                'match_analyzer',
                42,
                'true',
                32,
                'fuzzy_rewrite',
                'true',
                43,
                3.9,
                'true',
                'none',
                'and'
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "match": {
                        "match_field": {
                            "query": "match_query",
                            "boost": boost,
                            "analyzer": "match_analyzer",
                            "minimum_should_match": 42,
                            "lenient": true,
                            "fuzziness": 32,
                            "fuzzy_rewrite": "fuzzy_rewrite",
                            "fuzzy_transpositions": true,
                            "prefix_length": 43,
                            "cutoff_frequency": cutoff,
                            "auto_generate_synonyms_phrase_query": true,
                            "zero_terms_query": "none",
                            "operator": "and"
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_match_with_defaults() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.match(
                'match_field',
                'match_query'
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "match": {
                        "match_field": {
                            "query": "match_query",
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_multi_match_with_defaults() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.multi_match(
                    ARRAY [
                        'one',
                        'two',
                        'three'
                    ],
                    'match_query'
                )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "multi_match": {
                            "fields":["one", "two", "three"],
                            "query": "match_query",
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_multi_match_without_defaults() {
        let boost = 2.0 as f32;
        let cutoff = 3.9 as f32;
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.multi_match(
                    ARRAY [
                        'one',
                        'two',
                        'three'
                    ],
                    'match_query',
                    2.0,
                    'match_analyzer',
                    42,
                    'true',
                    32,
                    'fuzzy_rewrite',
                    'true',
                    43,
                    3.9,
                    'true',
                    'none',
                    'and',
                    'best_fields'
                )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "multi_match": {
                            "fields":["one", "two", "three"],
                            "query": "match_query",
                            "boost": boost,
                            "analyzer": "match_analyzer",
                            "minimum_should_match": 42,
                            "lenient": true,
                            "fuzziness": 32,
                            "fuzzy_rewrite": "fuzzy_rewrite",
                            "fuzzy_transpositions": true,
                            "prefix_length": 43,
                            "cutoff_frequency": cutoff,
                            "auto_generate_synonyms_phrase_query": true,
                            "zero_terms_query": "none",
                            "operator": "and",
                            "type": "best_fields"
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_match_phrase_prefix_with_defaults() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.match_phrase_prefix(
                'match_phrase_prefix_field',
                'match_phrase_prefix_query'
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "match_phrase_prefix": {
                        "match_phrase_prefix_field": {
                            "query": "match_phrase_prefix_query",
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_match_phrase_prefix_without_defaults() {
        let boost = 2.0 as f32;
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.match_phrase_prefix(
                'match_phrase_prefix_field',
                'match_phrase_prefix_query',
                '2.0',
                '54',
                'analyzer_phrase',
                '58',
                'none'
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "match_phrase_prefix": {
                        "match_phrase_prefix_field": {
                            "query": "match_phrase_prefix_query",
                            "boost": boost,
                            "slop": 54,
                            "analyzer": "analyzer_phrase",
                            "max_expansions": 58,
                            "zero_terms_query": "none",
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_match_phrase_with_defaults() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.match_phrase(
                'match_phrase_field',
                'match_phrase_query'
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "match_phrase": {
                        "match_phrase_field": {
                            "query": "match_phrase_query",
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_match_phrase_without_defaults() {
        let boost = 2.0 as f32;
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.match_phrase(
                'match_phrase_field',
                'match_phrase_query',
                '2.0',
                '54',
                'analyzer_phrase',
                'none'
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "match_phrase": {
                        "match_phrase_field": {
                            "query": "match_phrase_query",
                            "boost": boost,
                            "slop": 54,
                            "analyzer": "analyzer_phrase",
                            "zero_terms_query": "none",
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_phrase_with_defaults() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.phrase(
                'phrase_field',
                'phrase_query'
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "match_phrase": {
                        "phrase_field": {
                            "query": "phrase_query",
                        }
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_phrase_without_defaults() {
        let boost = 2.0 as f32;
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.phrase(
                'phrase_field',
                'phrase_query',
                '2.0',
                '54',
                'analyzer_phrase',
                'none'
            )",
        )
        .expect("SPI failed")
        .expect("SPI datum was NULL");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "match_phrase": {
                        "phrase_field": {
                            "query": "phrase_query",
                            "boost": boost,
                            "slop": 54,
                            "analyzer": "analyzer_phrase",
                            "zero_terms_query": "none",
                        }
                    }
                }
            }
        )
    }
}
