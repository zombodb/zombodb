mod dsl {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde::*;
    use serde_json::*;

    #[derive(Serialize)]
    struct MoreLikeThisArray<'a> {
        like: Array<'a, &'a str>,
        stop_words: Array<'a, &'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        fields: Option<Array<'a, &'a str>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        unlike: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        analyzer: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        minimum_should_match: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost_terms: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min_term_freq: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_query_terms: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min_doc_freq: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_doc_freq: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min_word_length: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_word_length: Option<i64>,
    }

    #[derive(Serialize)]
    struct MoreLikeThis<'a> {
        like: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        fields: Option<Array<'a, &'a str>>,
        stop_words: Array<'a, &'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        unlike: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        analyzer: Option<&'a str>,
        #[serde(skip_serializing_if = "Option::is_none")]
        minimum_should_match: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        boost_terms: Option<f32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        include: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min_term_freq: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_query_terms: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min_doc_freq: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_doc_freq: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        min_word_length: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_word_length: Option<i64>,
    }

    /// ```funcname
    /// more_like_this
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    fn more_like_this_with_array(
        like: Array<&str>,
        stop_words:
            default!(Array<&str>, "ARRAY['http', 'span', 'class', 'flashtext', 'let', 'its', 'may', 'well', 'got', 'too', 'them', 'really', 'new', 'set', 'please', 'how', 'our', 'from', 'sent', 'subject', 'sincerely', 'thank', 'thanks', 'just', 'get', 'going', 'were', 'much', 'can', 'also', 'she', 'her', 'him', 'his', 'has', 'been', 'ok', 'still', 'okay', 'does', 'did', 'about', 'yes', 'you', 'your', 'when', 'know', 'have', 'who', 'what', 'where', 'sir', 'page', 'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by', 'for', 'if', 'in', 'into', 'is', 'it', 'no', 'not', 'of', 'on', 'or', 'such', 'that', 'the', 'their', 'than', 'then', 'there', 'these', 'they', 'this', 'to', 'was', 'will', 'with']"),
        fields: Option<default!(Array<&str>, "NULL")>,
        boost: Option<default!(f32, "NULL")>,
        unlike: Option<default!(&str, "NULL")>,
        analyzer: Option<default!(&str, "NULL")>,
        minimum_should_match: Option<default!(i32, "NULL")>,
        boost_terms: Option<default!(f32, "NULL")>,
        include: Option<default!(bool, "NULL")>,
        min_term_freq: Option<default!(i64, "NULL")>,
        max_query_terms: Option<default!(i64, "NULL")>,
        min_doc_freq: Option<default!(i64, "NULL")>,
        max_doc_freq: Option<default!(i64, "NULL")>,
        min_word_length: Option<default!(i64, "NULL")>,
        max_word_length: Option<default!(i64, "NULL")>,
    ) -> ZDBQuery {
        let morelikethis = MoreLikeThisArray {
            like,
            stop_words,
            fields,
            boost,
            unlike,
            analyzer,
            minimum_should_match,
            boost_terms,
            include,
            min_term_freq,
            max_query_terms,
            min_doc_freq,
            max_doc_freq,
            min_word_length,
            max_word_length,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "more_like_this":
                     morelikethis
            }
        })
    }

    /// ```funcname
    /// more_like_this
    /// ```
    #[pg_extern(immutable, parallel_safe)]
    fn more_like_this_without_array(
        like: &str,
        fields: Option<default!(Array<&str>, "NULL")>,
        stop_words:
            default!(Array<&str>, "ARRAY['http', 'span', 'class', 'flashtext', 'let', 'its', 'may', 'well', 'got', 'too', 'them', 'really', 'new', 'set', 'please', 'how', 'our', 'from', 'sent', 'subject', 'sincerely', 'thank', 'thanks', 'just', 'get', 'going', 'were', 'much', 'can', 'also', 'she', 'her', 'him', 'his', 'has', 'been', 'ok', 'still', 'okay', 'does', 'did', 'about', 'yes', 'you', 'your', 'when', 'know', 'have', 'who', 'what', 'where', 'sir', 'page', 'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by', 'for', 'if', 'in', 'into', 'is', 'it', 'no', 'not', 'of', 'on', 'or', 'such', 'that', 'the', 'their', 'than', 'then', 'there', 'these', 'they', 'this', 'to', 'was', 'will', 'with']"),
        boost: Option<default!(f32, "NULL")>,
        unlike: Option<default!(&str, "NULL")>,
        analyzer: Option<default!(&str, "NULL")>,
        minimum_should_match: Option<default!(i32, "NULL")>,
        boost_terms: Option<default!(f32, "NULL")>,
        include: Option<default!(bool, "NULL")>,
        min_term_freq: Option<default!(i64, "NULL")>,
        max_query_terms: Option<default!(i64, "NULL")>,
        min_doc_freq: Option<default!(i64, "NULL")>,
        max_doc_freq: Option<default!(i64, "NULL")>,
        min_word_length: Option<default!(i64, "NULL")>,
        max_word_length: Option<default!(i64, "NULL")>,
    ) -> ZDBQuery {
        let morelikethis = MoreLikeThis {
            like,
            fields,
            stop_words,
            boost,
            unlike,
            analyzer,
            minimum_should_match,
            boost_terms,
            include,
            min_term_freq,
            max_query_terms,
            min_doc_freq,
            max_doc_freq,
            min_word_length,
            max_word_length,
        };
        ZDBQuery::new_with_query_dsl(json! {
            {
                "more_like_this":
                     morelikethis
            }
        })
    }
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::zdbquery::ZDBQuery;
    use pgx::*;
    use serde_json::*;

    #[pg_test]
    fn test_more_like_this_with_inputs() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.more_like_this(
                'like_string',
                ARRAY['fieldsone','fieldstwo','fieldsthree'],
                ARRAY['stop','words'],
                4.5,
                'unliked string',
                'analyzer string',
                42,
                6.7,
                'true',
                1,
                2,
                3,
                4,
                5,
                6
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "more_like_this": {
                        "like": "like_string",
                        "fields": ["fieldsone","fieldstwo","fieldsthree"],
                        "stop_words": ["stop","words"],
                        "boost": 4.5 as f32,
                        "unlike": "unliked string",
                        "analyzer": "analyzer string",
                        "minimum_should_match": 42,
                        "boost_terms": 6.7 as f32,
                        "include": true,
                        "min_term_freq": 1,
                        "max_query_terms": 2,
                        "min_doc_freq": 3,
                        "max_doc_freq": 4,
                        "min_word_length": 5,
                        "max_word_length": 6,
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_more_like_this_with_defaults() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.more_like_this(
                'like_string'
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "more_like_this": {
                        "like": "like_string",
                        "stop_words": ["http", "span", "class", "flashtext", "let", "its", "may", "well", "got", "too", "them", "really", "new", "set", "please", "how", "our", "from", "sent", "subject", "sincerely", "thank", "thanks", "just", "get", "going", "were", "much", "can", "also", "she", "her", "him", "his", "has", "been", "ok", "still", "okay", "does", "did", "about", "yes", "you", "your", "when", "know", "have", "who", "what", "where", "sir", "page", "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "than", "then", "there", "these", "they", "this", "to", "was", "will", "with"],
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_more_like_this_with_inputs_with_arrays() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.more_like_this(
                ARRAY['like_string uno', 'string like dos', 'like string three'],
                ARRAY['stop','words'],
                ARRAY['fieldsone','fieldstwo','fieldsthree'],
                4.5,
                'unliked string',
                'analyzer string',
                42,
                6.7,
                'true',
                1,
                2,
                3,
                4,
                5,
                6
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "more_like_this": {
                        "like": ["like_string uno", "string like dos", "like string three"],
                        "fields": ["fieldsone","fieldstwo","fieldsthree"],
                        "stop_words": ["stop","words"],
                        "boost": 4.5 as f32,
                        "unlike": "unliked string",
                        "analyzer": "analyzer string",
                        "minimum_should_match": 42,
                        "boost_terms": 6.7 as f32,
                        "include": true,
                        "min_term_freq": 1,
                        "max_query_terms": 2,
                        "min_doc_freq": 3,
                        "max_doc_freq": 4,
                        "min_word_length": 5,
                        "max_word_length": 6,
                    }
                }
            }
        )
    }

    #[pg_test]
    fn test_more_like_this_with_defaults_with_arrays() {
        let zdbquery = Spi::get_one::<ZDBQuery>(
            "SELECT dsl.more_like_this(
                Array['like_string uno', 'string like dos', 'like string three']
            )",
        )
        .expect("failed to get SPI result");
        let dsl = zdbquery.into_value();

        assert_eq!(
            dsl,
            json! {
                {
                    "more_like_this": {
                        "like": ["like_string uno", "string like dos", "like string three"],
                        "stop_words": ["http", "span", "class", "flashtext", "let", "its", "may", "well", "got", "too", "them", "really", "new", "set", "please", "how", "our", "from", "sent", "subject", "sincerely", "thank", "thanks", "just", "get", "going", "were", "much", "can", "also", "she", "her", "him", "his", "has", "been", "ok", "still", "okay", "does", "did", "about", "yes", "you", "your", "when", "know", "have", "who", "what", "where", "sir", "page", "a", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their", "than", "then", "there", "these", "they", "this", "to", "was", "will", "with"],
                    }
                }
            }
        )
    }
}
