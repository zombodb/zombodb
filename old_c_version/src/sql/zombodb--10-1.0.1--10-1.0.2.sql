-- drop all the functions we're about to replace
DROP FUNCTION zdb.analyze_with_field;
DROP FUNCTION zdb.define_field_mapping;
DROP FUNCTION zdb.define_es_only_field;
DROP FUNCTION dsl.join;

DROP FUNCTION dsl.term(name, text, real);
DROP FUNCTION dsl.term(name, numeric, real);
DROP FUNCTION dsl.terms(name, VARIADIC text[]);
DROP FUNCTION dsl.terms(name, VARIADIC numeric[]);
DROP FUNCTION dsl.terms_array;
DROP FUNCTION dsl.terms_lookup;
DROP FUNCTION dsl.range(name, numeric, numeric, numeric, numeric, real);
DROP FUNCTION dsl.range(name, text, text, text, text, real);
DROP FUNCTION dsl.prefix;
DROP FUNCTION dsl.wildcard;
DROP FUNCTION dsl.regexp;
DROP FUNCTION dsl.fuzzy;
DROP FUNCTION dsl.match;
DROP FUNCTION dsl.match_phrase;
DROP FUNCTION dsl.phrase;
DROP FUNCTION dsl.match_phrase_prefix;
DROP FUNCTION dsl.common;
DROP FUNCTION dsl.nested;
DROP FUNCTION dsl.span_term;
DROP FUNCTION dsl.span_masking;
DROP TYPE dsl.esqdsl_nested;
DROP FUNCTION zdb.highlight(tid, name, json);

--
-- replace functions and types
--

ALTER TABLE zdb.mappings ALTER COLUMN field_name TYPE text USING field_name::text;

CREATE OR REPLACE FUNCTION zdb.analyze_with_field(index regclass, field text, text text) RETURNS TABLE (type text, token text, "position" int, start_offset int, end_offset int) PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT tokens->>'type',
           tokens->>'token',
           (tokens->>'position')::int,
           (tokens->>'start_offset')::int,
           (tokens->>'end_offset')::int
      FROM jsonb_array_elements((zdb.request(index, '_analyze', 'GET', json_build_object('field', field, 'text', text)::text)::jsonb)->'tokens') tokens;
$$;

CREATE OR REPLACE FUNCTION zdb.define_field_mapping(table_name regclass, field_name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.mappings WHERE table_name = $1 AND field_name = $2;
  INSERT INTO zdb.mappings(table_name, field_name, definition) VALUES ($1, $2, $3);
$$;

CREATE OR REPLACE FUNCTION zdb.define_es_only_field(table_name regclass, field_name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.mappings WHERE table_name = $1 AND field_name = $2;
  INSERT INTO zdb.mappings(table_name, field_name, definition, es_only) VALUES ($1, $2, $3, true);
$$;

CREATE OR REPLACE FUNCTION dsl.join(left_field text, index regclass, right_field text, query zdbquery, size int DEFAULT 0) RETURNS zdbquery PARALLEL SAFE STABLE LANGUAGE plpgsql AS $$
BEGIN
    IF size > 0 THEN
        /* if we have a size limit, then limit to the top matching hits */
        RETURN dsl.filter(dsl.terms(left_field, VARIADIC (SELECT array_agg(source->>right_field) FROM zdb.top_hits(index, ARRAY[right_field], query, size))));
    ELSE
        /* otherwise, return all the matching terms */
        RETURN dsl.filter(dsl.terms(left_field, VARIADIC zdb.terms_array(index, right_field, query)));
    END IF;
END;
$$;

-- query dsl changed

CREATE OR REPLACE FUNCTION dsl.term(field text, value text, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('term', json_build_object(field, ROW(value, boost)::dsl.esqdsl_term_text)))::zdbquery;
$$;
CREATE OR REPLACE FUNCTION dsl.term(field text, value numeric, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('term', json_build_object(field, ROW(value, boost)::dsl.esqdsl_term_numeric)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.terms(field text, VARIADIC "values" text[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, "values")))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.terms(field text, VARIADIC "values" numeric[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, "values")))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.terms_array(field text, "values" anyarray) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, "values")))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.terms_lookup(field text, index text, type text, path text, id text) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, ROW(index, type, path, id)::dsl.esqdsl_terms_lookup)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.range(field text, lt text DEFAULT NULL, gt text DEFAULT NULL, lte text DEFAULT NULL, gte text DEFAULT NULL, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('range', json_build_object(field, ROW(lt, gt, lte, gte, boost)::dsl.esqdsl_range_text)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.range(field text, lt numeric DEFAULT NULL, gt numeric DEFAULT NULL, lte numeric DEFAULT NULL, gte numeric DEFAULT NULL, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('range', json_build_object(field, ROW(lt, gt, lte, gte, boost)::dsl.esqdsl_range_text)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.prefix(field text, prefix text, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('prefix', json_build_object(field, ROW(prefix, boost)::dsl.esqdsl_prefix)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.wildcard(field text, wildcard text, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('wildcard', json_build_object(field, ROW(wildcard, boost)::dsl.esqdsl_wildcard)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.regexp(field text, regexp text, boost real DEFAULT NULL, flags dsl.es_regexp_flags[] DEFAULT NULL, max_determinized_states int DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('regexp', json_build_object(field, ROW(regexp, (SELECT array_to_string(array_agg(DISTINCT flag), '|') FROM unnest(flags) flag), max_determinized_states, boost)::dsl.esqdsl_regexp)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.fuzzy(field text, value text, boost real DEFAULT NULL, fuzziness int DEFAULT NULL, prefix_length int DEFAULT NULL, max_expansions int DEFAULT NULL, transpositions boolean DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('fuzzy', json_build_object(field, ROW(value, boost, fuzziness, prefix_length, max_expansions, transpositions)::dsl.esqdsl_fuzzy)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.match(field text, query text, boost real DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match text DEFAULT NULL, lenient boolean DEFAULT NULL, fuzziness int DEFAULT NULL, fuzzy_rewrite text DEFAULT NULL, fuzzy_transpositions boolean DEFAULT NULL, prefix_length int DEFAULT NULL, zero_terms_query dsl.es_match_zero_terms_query DEFAULT NULL, cutoff_frequency real DEFAULT NULL, operator dsl.es_match_operator DEFAULT NULL, auto_generate_synonyms_phrase_query boolean DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('match', json_build_object(field, ROW(query, boost, analyzer, minimum_should_match, lenient, fuzziness, fuzzy_rewrite, fuzzy_transpositions, prefix_length, zero_terms_query, cutoff_frequency, operator, auto_generate_synonyms_phrase_query)::dsl.esqdsl_match)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.match_phrase(field text, query text, boost real DEFAULT NULL, slop int DEFAULT NULL, analyzer text DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('match_phrase', json_build_object(field, ROW(query, boost, slop, analyzer)::dsl.esqdsl_match_phrase)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.phrase(field text, query text, boost real DEFAULT NULL, slop int DEFAULT NULL, analyzer text DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.match_phrase(field, query, boost, slop, analyzer);
$$;

CREATE OR REPLACE FUNCTION dsl.match_phrase_prefix(field text, query text, boost real DEFAULT NULL, slop int DEFAULT NULL, analyzer text DEFAULT NULL, max_expansions int DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('match_phrase_prefix', json_build_object(field, ROW(query, boost, slop, analyzer, max_expansions)::dsl.esqdsl_match_phrase_prefix)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.common(field text, query text, boost real DEFAULT NULL, cutoff_frequency real DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match text DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('common', json_build_object(field, ROW(query, boost, cutoff_frequency, analyzer, minimum_should_match)::dsl.esqdsl_common)))::zdbquery;
$$;

CREATE TYPE dsl.esqdsl_nested AS (path text, query zdbquery, score_mode dsl.es_nested_score_mode);
CREATE OR REPLACE FUNCTION dsl.nested(path text, query zdbquery, score_mode dsl.es_nested_score_mode DEFAULT 'avg'::dsl.es_nested_score_mode) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('nested', ROW(path, query, score_mode)::dsl.esqdsl_nested))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.span_term(field text, value text, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('span_term', json_build_object(field, ROW(value, boost)::dsl.esqdsl_term_text)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.span_masking(field text, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('field_masking_span', json_build_object('query', query, 'field', field)))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION highlight(ctid tid, field text, highlight_definition json DEFAULT highlight()) RETURNS text[] PARALLEL UNSAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_highlight';



