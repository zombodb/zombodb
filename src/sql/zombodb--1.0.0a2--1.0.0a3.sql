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
