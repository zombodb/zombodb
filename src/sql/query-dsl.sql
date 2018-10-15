CREATE OR REPLACE FUNCTION to_query_dsl(zdbquery) RETURNS json PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_to_query_dsl';
CREATE OR REPLACE FUNCTION to_queries_dsl(queries zdbquery[]) RETURNS json[] PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT array_agg(zdb.to_query_dsl(query)) FROM unnest(queries) query;
$$;

--
-- query composition functions
--
-- all of these should be created in the 'dsl' schema
--
CREATE SCHEMA dsl;
GRANT ALL ON SCHEMA dsl TO PUBLIC;

CREATE OR REPLACE FUNCTION dsl.match_all(boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('match_all', json_build_object('boost', boost)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.match_none() RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('match_none', '{}'::json))::zdbquery;
$$;

CREATE TYPE dsl.esqdsl_bool AS (should zdbquery[], must zdbquery[], must_not zdbquery[], filter zdbquery[]);
CREATE OR REPLACE FUNCTION dsl.should(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('bool', ROW(queries, NULL, NULL, NULL)::dsl.esqdsl_bool))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.must(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('bool', ROW(NULL, queries, NULL, NULL)::dsl.esqdsl_bool))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.must_not(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('bool', ROW(NULL, NULL, queries, NULL)::dsl.esqdsl_bool))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.filter(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('bool', ROW(NULL, NULL, NULL, queries)::dsl.esqdsl_bool))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.noteq(query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.must_not(VARIADIC ARRAY[query]);
$$;


CREATE TYPE dsl.esqdsl_term_text AS (value text, boost real);
CREATE TYPE dsl.esqdsl_term_numeric AS (value numeric, boost real);
CREATE OR REPLACE FUNCTION dsl.term(field name, value text, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('term', json_build_object(field, ROW(value, boost)::dsl.esqdsl_term_text)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.term(field name, value numeric, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('term', json_build_object(field, ROW(value, boost)::dsl.esqdsl_term_numeric)))::zdbquery;
$$;


CREATE OR REPLACE FUNCTION dsl.terms(field name, VARIADIC "values" text[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, "values")))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.terms(field name, VARIADIC "values" numeric[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, "values")))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.terms_array(field name, "values" anyarray) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, "values")))::zdbquery;
$$;


CREATE TYPE dsl.esqdsl_terms_lookup AS (index text, type text, path text, id text);
CREATE OR REPLACE FUNCTION dsl.terms_lookup(field name, index text, type text, path text, id text) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, ROW(index, type, path, id)::dsl.esqdsl_terms_lookup)))::zdbquery;
$$;


CREATE TYPE dsl.esqdsl_range_text AS (lt text, gt text, lte text, gte text, boost real);
CREATE TYPE dsl.esqdsl_range_numeric AS (lt numeric, gt numeric, lte numeric, gte numeric, boost real);
CREATE OR REPLACE FUNCTION dsl.range(field name, lt text DEFAULT NULL, gt text DEFAULT NULL, lte text DEFAULT NULL, gte text DEFAULT NULL, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('range', json_build_object(field, ROW(lt, gt, lte, gte, boost)::dsl.esqdsl_range_text)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.range(field name, lt numeric DEFAULT NULL, gt numeric DEFAULT NULL, lte numeric DEFAULT NULL, gte numeric DEFAULT NULL, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('range', json_build_object(field, ROW(lt, gt, lte, gte, boost)::dsl.esqdsl_range_text)))::zdbquery;
$$;


CREATE TYPE dsl.esqdsl_exists AS (field name);
CREATE OR REPLACE FUNCTION dsl.field_exists(field name) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('exists', ROW(field)::dsl.esqdsl_exists))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.field_missing(field name) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.noteq(dsl.field_exists(field));
$$;


CREATE TYPE dsl.esqdsl_prefix AS (value text, boost real);
CREATE OR REPLACE FUNCTION dsl.prefix(field name, prefix text, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('prefix', json_build_object(field, ROW(prefix, boost)::dsl.esqdsl_prefix)))::zdbquery;
$$;


CREATE TYPE dsl.esqdsl_wildcard AS (value text, boost real);
CREATE OR REPLACE FUNCTION dsl.wildcard(field name, wildcard text, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('wildcard', json_build_object(field, ROW(wildcard, boost)::dsl.esqdsl_wildcard)))::zdbquery;
$$;


CREATE TYPE dsl.es_regexp_flags AS ENUM ('ALL', 'ANYSTRING', 'COMPLEMENT', 'EMPTY', 'INTERSECTION', 'INTERVAL', 'NONE');
CREATE TYPE dsl.esqdsl_regexp AS (value text, flags text, max_determined_states int, boost real);
CREATE OR REPLACE FUNCTION dsl.regexp(field name, regexp text, boost real DEFAULT NULL, flags dsl.es_regexp_flags[] DEFAULT NULL, max_determinized_states int DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('regexp', json_build_object(field, ROW(regexp, (SELECT array_to_string(array_agg(DISTINCT flag), '|') FROM unnest(flags) flag), max_determinized_states, boost)::dsl.esqdsl_regexp)))::zdbquery;
$$;


CREATE TYPE dsl.esqdsl_fuzzy AS (value text, boost real, fuzziness int, prefix_length int, max_expansions int, transpositions boolean);
CREATE OR REPLACE FUNCTION dsl.fuzzy(field name, value text, boost real DEFAULT NULL, fuzziness int DEFAULT NULL, prefix_length int DEFAULT NULL, max_expansions int DEFAULT NULL, transpositions boolean DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('fuzzy', json_build_object(field, ROW(value, boost, fuzziness, prefix_length, max_expansions, transpositions)::dsl.esqdsl_fuzzy)))::zdbquery;
$$;


CREATE TYPE dsl.es_match_zero_terms_query AS ENUM ('none', 'all');
CREATE TYPE dsl.es_match_operator AS ENUM ('and', 'or');
CREATE TYPE dsl.esqdsl_match AS (query text, boost real, analyzer text, minimum_should_match text, lenient boolean, fuzziness int, fuzzy_rewrite text, fuzzy_transpositions boolean, prefix_length int, zero_terms_query dsl.es_match_zero_terms_query, cutoff_frequency real, operator dsl.es_match_operator, auto_generate_synonyms_phrase_query boolean);
CREATE OR REPLACE FUNCTION dsl.match(field name, query text, boost real DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match text DEFAULT NULL, lenient boolean DEFAULT NULL, fuzziness int DEFAULT NULL, fuzzy_rewrite text DEFAULT NULL, fuzzy_transpositions boolean DEFAULT NULL, prefix_length int DEFAULT NULL, zero_terms_query dsl.es_match_zero_terms_query DEFAULT NULL, cutoff_frequency real DEFAULT NULL, operator dsl.es_match_operator DEFAULT NULL, auto_generate_synonyms_phrase_query boolean DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('match', json_build_object(field, ROW(query, boost, analyzer, minimum_should_match, lenient, fuzziness, fuzzy_rewrite, fuzzy_transpositions, prefix_length, zero_terms_query, cutoff_frequency, operator, auto_generate_synonyms_phrase_query)::dsl.esqdsl_match)))::zdbquery;
$$;


CREATE TYPE dsl.esqdsl_match_phrase AS (query text, boost real, slop int, analyzer text);
CREATE OR REPLACE FUNCTION dsl.match_phrase(field name, query text, boost real DEFAULT NULL, slop int DEFAULT NULL, analyzer text DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('match_phrase', json_build_object(field, ROW(query, boost, slop, analyzer)::dsl.esqdsl_match_phrase)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.phrase(field name, query text, boost real DEFAULT NULL, slop int DEFAULT NULL, analyzer text DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.match_phrase(field, query, boost, slop, analyzer);
$$;


CREATE TYPE dsl.esqdsl_match_phrase_prefix AS (query text, boost real, slop int, analyzer text, max_expansions int);
CREATE OR REPLACE FUNCTION dsl.match_phrase_prefix(field name, query text, boost real DEFAULT NULL, slop int DEFAULT NULL, analyzer text DEFAULT NULL, max_expansions int DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('match_phrase_prefix', json_build_object(field, ROW(query, boost, slop, analyzer, max_expansions)::dsl.esqdsl_match_phrase_prefix)))::zdbquery;
$$;


CREATE TYPE dsl.es_multi_match_type AS ENUM ('best_fields', 'most_fields', 'cross_fields', 'phrase', 'phrase_prefix');
CREATE TYPE dsl.esqdsl_multi_match AS (fields name[], type dsl.es_multi_match_type, query text, boost real, analyzer text, minimum_should_match text, lenient boolean, fuzziness int, fuzzy_rewrite text, fuzzy_transpositions boolean, prefix_length int, zero_terms_query dsl.es_match_zero_terms_query, cutoff_frequency real, operator dsl.es_match_operator, auto_generate_synonyms_phrase_query boolean);
CREATE OR REPLACE FUNCTION dsl.multi_match(fields name[], query text, boost real DEFAULT NULL, type dsl.es_multi_match_type DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match text DEFAULT NULL, lenient boolean DEFAULT NULL, fuzziness int DEFAULT NULL, fuzzy_rewrite text DEFAULT NULL, fuzzy_transpositions boolean DEFAULT NULL, prefix_length int DEFAULT NULL, zero_terms_query dsl.es_match_zero_terms_query DEFAULT NULL, cutoff_frequency real DEFAULT NULL, operator dsl.es_match_operator DEFAULT NULL, auto_generate_synonyms_phrase_query boolean DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('multi_match', ROW(fields, type, query, boost, analyzer, minimum_should_match, lenient, fuzziness, fuzzy_rewrite, fuzzy_transpositions, prefix_length, zero_terms_query, cutoff_frequency, operator, auto_generate_synonyms_phrase_query)::dsl.esqdsl_multi_match))::zdbquery;
$$;


CREATE TYPE dsl.esqdsl_common AS (query text, boost real, cutoff_frequency real, analyzer text, minimum_should_match text);
CREATE OR REPLACE FUNCTION dsl.common(field name, query text, boost real DEFAULT NULL, cutoff_frequency real DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match text DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('common', json_build_object(field, ROW(query, boost, cutoff_frequency, analyzer, minimum_should_match)::dsl.esqdsl_common)))::zdbquery;
$$;


CREATE OR REPLACE FUNCTION dsl.constant_score(query zdbquery, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('constant_score', json_build_object('filter', query), 'boost', boost))::zdbquery;
$$;

CREATE TYPE dsl.esqdsl_dis_max AS (queries zdbquery[], tie_breaker real, boost real);
CREATE OR REPLACE FUNCTION dsl.dis_max(queries zdbquery[], boost real DEFAULT NULL, tie_breaker real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('dis_max', ROW(queries, tie_breaker, boost)::dsl.esqdsl_dis_max))::zdbquery;
$$;


CREATE TYPE dsl.esqdsl_more_like_this AS (fields name[], "like" text[], boost real, unlike text, analyzer text, stop_words text[], minimum_should_match text, boost_terms real, include boolean, min_term_freq int, max_query_terms int, min_doc_freq int, max_doc_freq int, min_word_length int, max_word_length int);
CREATE OR REPLACE FUNCTION dsl.more_like_this("like" text[], fields name[] DEFAULT NULL, stop_words text[] DEFAULT ARRAY[
            'http', 'span', 'class', 'flashtext', 'let', 'its',
            'may', 'well', 'got', 'too', 'them', 'really', 'new', 'set', 'please',
            'how', 'our', 'from', 'sent', 'subject', 'sincerely', 'thank', 'thanks',
            'just', 'get', 'going', 'were', 'much', 'can', 'also', 'she', 'her',
            'him', 'his', 'has', 'been', 'ok', 'still', 'okay', 'does', 'did',
            'about', 'yes', 'you', 'your', 'when', 'know', 'have', 'who', 'what',
            'where', 'sir', 'page', 'a', 'an', 'and', 'are', 'as', 'at', 'be',
            'but', 'by', 'for', 'if', 'in', 'into', 'is', 'it', 'no', 'not', 'of',
            'on', 'or', 'such', 'that', 'the', 'their', 'than', 'then', 'there',
            'these', 'they', 'this', 'to', 'was', 'will', 'with'], boost real DEFAULT NULL, unlike text DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match text DEFAULT NULL, boost_terms real DEFAULT NULL, include boolean DEFAULT NULL, min_term_freq int DEFAULT NULL, max_query_terms int DEFAULT NULL, min_doc_freq int DEFAULT NULL, max_doc_freq int DEFAULT NULL, min_word_length int DEFAULT NULL, max_word_length int DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('more_like_this', ROW(fields, "like", boost, unlike, analyzer, stop_words, minimum_should_match, boost_terms, include, min_term_freq, max_query_terms, min_doc_freq, max_doc_freq, min_word_length, max_word_length)::dsl.esqdsl_more_like_this))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.more_like_this("like" text, fields name[] DEFAULT NULL, stop_words text[] DEFAULT ARRAY[
            'http', 'span', 'class', 'flashtext', 'let', 'its',
            'may', 'well', 'got', 'too', 'them', 'really', 'new', 'set', 'please',
            'how', 'our', 'from', 'sent', 'subject', 'sincerely', 'thank', 'thanks',
            'just', 'get', 'going', 'were', 'much', 'can', 'also', 'she', 'her',
            'him', 'his', 'has', 'been', 'ok', 'still', 'okay', 'does', 'did',
            'about', 'yes', 'you', 'your', 'when', 'know', 'have', 'who', 'what',
            'where', 'sir', 'page', 'a', 'an', 'and', 'are', 'as', 'at', 'be',
            'but', 'by', 'for', 'if', 'in', 'into', 'is', 'it', 'no', 'not', 'of',
            'on', 'or', 'such', 'that', 'the', 'their', 'than', 'then', 'there',
            'these', 'they', 'this', 'to', 'was', 'will', 'with'], boost real DEFAULT NULL, unlike text DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match text DEFAULT NULL, boost_terms real DEFAULT NULL, include boolean DEFAULT NULL, min_term_freq int DEFAULT NULL, max_query_terms int DEFAULT NULL, min_doc_freq int DEFAULT NULL, max_doc_freq int DEFAULT NULL, min_word_length int DEFAULT NULL, max_word_length int DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('more_like_this', ROW(fields, ARRAY["like"], boost, unlike, analyzer, stop_words, minimum_should_match, boost_terms, include, min_term_freq, max_query_terms, min_doc_freq, max_doc_freq, min_word_length, max_word_length)::dsl.esqdsl_more_like_this))::zdbquery;
$$;

CREATE TYPE dsl.esqdsl_script AS (source text, lang text, params json);
CREATE FUNCTION dsl.params(VARIADIC "any") RETURNS json PARALLEL SAFE IMMUTABLE LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_json_build_object_wrapper';
CREATE OR REPLACE FUNCTION dsl.script(source_code text, params json DEFAULT NULL, lang text DEFAULT 'painless') RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('script', json_build_object('script', ROW(source_code, lang, params)::dsl.esqdsl_script)))::zdbquery;
$$;

CREATE TYPE dsl.esqdsl_boosting AS (positive zdbquery, negative zdbquery, negative_boost real);
CREATE OR REPLACE FUNCTION dsl.boosting(positive zdbquery, negative zdbquery, negative_boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('boosting', ROW(positive, negative, negative_boost)::dsl.esqdsl_boosting))::zdbquery;
$$;

CREATE TYPE dsl.es_nested_score_mode AS ENUM ('avg', 'sum', 'min', 'max', 'none');
CREATE TYPE dsl.esqdsl_nested AS (path name, query zdbquery, score_mode dsl.es_nested_score_mode);
CREATE OR REPLACE FUNCTION dsl.nested(path name, query zdbquery, score_mode dsl.es_nested_score_mode DEFAULT 'avg'::dsl.es_nested_score_mode) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('nested', ROW(path, query, score_mode)::dsl.esqdsl_nested))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.span_term(field name, value text, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_term', json_build_object(field, ROW(value, boost)::dsl.esqdsl_term_text)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.span_multi(query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_multi', json_build_object('match', query)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.span_first(query zdbquery, "end" int) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_first', json_build_object('match', query), 'end', "end"))::zdbquery;
$$;

CREATE TYPE dsl.esqdsl_span_near AS (clauses zdbquery[], slop int, in_order boolean);
CREATE OR REPLACE FUNCTION dsl.span_near(in_order boolean, slop int, VARIADIC clauses zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_near', ROW(clauses, slop, in_order)::dsl.esqdsl_span_near))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.span_or(VARIADIC clauses zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_or', json_build_object('clauses', clauses)))::zdbquery;
$$;

CREATE TYPE dsl.esqdsl_span_not AS (include zdbquery, exclude zdbquery, pre int, post int, dist int);
CREATE OR REPLACE FUNCTION dsl.span_not(include zdbquery, exclude zdbquery, pre int DEFAULT NULL, post int DEFAULT NULL, dist int DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_not', ROW(include, exclude, pre, post, dist)::dsl.esqdsl_span_not))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.span_containing(little zdbquery, big zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_containing', json_build_object('little', little, 'big', big)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.span_within(little zdbquery, big zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_within', json_build_object('little', little, 'big', big)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.span_masking(field name, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('field_masking_span', json_build_object('query', query, 'field', field)))::zdbquery;
$$;

CREATE TYPE dsl.esqdsl_default_operators AS ENUM ('AND', 'OR');
CREATE TYPE dsl.esqdsl_query_string AS (query text, default_field text, default_operator dsl.esqdsl_default_operators, analyzer text, quote_analyzer text, allow_leading_wildcard boolean, enable_position_increments boolean, fuzzy_max_expansions integer, fuzziness text, fuzzy_prefix_length integer, fuzzy_transpositions boolean, phrase_slop integer, boost real, auto_generate_phrase_queries boolean, analyze_wildcard boolean, max_determinized_states integer, minimum_should_match integer, lenient boolean, time_zone text, quote_field_suffix text, auto_generate_synonyms_phrase_query boolean, all_fields boolean);
CREATE OR REPLACE FUNCTION dsl.query_string(
    query text,
    default_operator dsl.esqdsl_default_operators DEFAULT NULL,
    default_field text DEFAULT NULL,
    analyzer text DEFAULT NULL,
    quote_analyzer text DEFAULT NULL,
    allow_leading_wildcard boolean DEFAULT NULL,
    enable_position_increments boolean DEFAULT NULL,
    fuzzy_max_expansions integer DEFAULT NULL,
    fuzziness text DEFAULT NULL,
    fuzzy_prefix_length integer DEFAULT NULL,
    fuzzy_transpositions boolean DEFAULT NULL,
    phrase_slop integer DEFAULT NULL,
    boost real DEFAULT NULL,
    auto_generate_phrase_queries boolean DEFAULT NULL,
    analyze_wildcard boolean DEFAULT NULL,
    max_determinized_states integer DEFAULT NULL,
    minimum_should_match integer DEFAULT NULL,
    lenient boolean DEFAULT NULL,
    time_zone text DEFAULT NULL,
    quote_field_suffix text DEFAULT NULL,
    auto_generate_synonyms_phrase_query boolean DEFAULT NULL,
    all_fields boolean DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('query_string', ROW(query, default_field, default_operator, analyzer,
                                quote_analyzer, allow_leading_wildcard, enable_position_increments, fuzzy_max_expansions,
                                fuzziness, fuzzy_prefix_length, fuzzy_transpositions, phrase_slop, boost,
                                auto_generate_phrase_queries, analyze_wildcard, max_determinized_states,
                                minimum_should_match, lenient, time_zone, quote_field_suffix,
                                auto_generate_synonyms_phrase_query, all_fields)::dsl.esqdsl_query_string))::zdbquery;
$$;
