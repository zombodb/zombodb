DROP FUNCTION IF EXISTS schema_version() CASCADE;
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.0.0-beta1 (ab62e48f53a9832bfd0df048f0aa0b3813780eca)'
$$;
INSERT INTO zdb.filters(name, definition, is_default)
VALUES ('zdb_truncate_to_fit', '{
  "type": "truncate",
  "length": 10922
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.filters(name, definition, is_default)
VALUES ('shingle_filter', '{
  "type": "shingle",
  "min_shingle_size": 2,
  "max_shingle_size": 2,
  "output_unigrams": true,
  "token_separator": "$"
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.filters(name, definition, is_default)
VALUES ('shingle_filter_search', '{
  "type": "shingle",
  "min_shingle_size": 2,
  "max_shingle_size": 2,
  "output_unigrams": false,
  "output_unigrams_if_no_shingles": true,
  "token_separator": "$"
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.normalizers(name, definition, is_default)
VALUES ('lowercase', '{
  "type": "custom",
  "char_filter": [],
  "filter": [
    "lowercase"
  ]
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
-- same as 'lowercase' for backwards compatibility
INSERT INTO zdb.normalizers(name, definition, is_default)
VALUES ('exact', '{
  "type": "custom",
  "char_filter": [],
  "filter": [
    "lowercase"
  ]
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.analyzers(name, definition, is_default)
VALUES ('zdb_standard', '{
  "type": "standard",
  "filter": [
    "zdb_truncate_to_fit",
    "lowercase"
  ]
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.analyzers(name, definition, is_default)
VALUES ('zdb_all_analyzer', '{
  "type": "standard",
  "filter": [
    "zdb_truncate_to_fit",
    "lowercase"
  ]
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.analyzers(name, definition, is_default)
VALUES ('fulltext_with_shingles', '{
  "type": "custom",
  "tokenizer": "standard",
  "filter": [
    "lowercase",
    "shingle_filter",
    "zdb_truncate_to_fit"
  ]
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.analyzers(name, definition, is_default)
VALUES ('fulltext_with_shingles_search', '{
  "type": "custom",
  "tokenizer": "standard",
  "filter": [
    "lowercase",
    "shingle_filter_search"
  ]
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.analyzers(name, definition, is_default)
VALUES ('fulltext', '{
  "type": "standard",
  "filter": [
    "zdb_truncate_to_fit",
    "lowercase"
  ]
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.analyzers(name, definition, is_default)
VALUES ('phrase', '{
  "type": "standard",
  "copy_to": "zdb_all",
  "filter": [
    "zdb_truncate_to_fit",
    "lowercase"
  ]
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('"char"', '{
  "type": "keyword"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('char', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "ignore_above": 10922,
  "normalizer": "lowercase"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('bytea', '{
  "type": "binary"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('boolean', '{
  "type": "boolean"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('smallint', '{
  "type": "short"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('integer', '{
  "type": "integer"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('bigint', '{
  "type": "long"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('real', '{
  "type": "float"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('double precision', '{
  "type": "double"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('character varying', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "ignore_above": 10922,
  "normalizer": "lowercase"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('text', '{
  "type": "text",
  "copy_to": "zdb_all",
  "fielddata": true,
  "index_prefixes": { },
  "analyzer": "zdb_standard"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
--INSERT INTO zdb.type_mappings(type_name, definition, is_default) VALUES (
--  'citext', '{
--    "type": "text",
--    "copy_to": "zdb_all",
--    "fielddata": true,
--    "analyzer": "zdb_standard"
--  }', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;

INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('time without time zone', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "fields": {
    "date": {
      "type": "date",
      "format": "HH:mm:ss.S||HH:mm:ss.SS||HH:mm:ss.SSS||HH:mm:ss.SSSS||HH:mm:ss.SSSSS||HH:mm:ss.SSSSSS"
    }
  }
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('time with time zone', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "fields": {
    "date": {
      "type": "date",
      "format": "HH:mm:ss.SX||HH:mm:ss.SSX||HH:mm:ss.SSSX||HH:mm:ss.SSSSX||HH:mm:ss.SSSSSX||HH:mm:ss.SSSSSSX"
    }
  }
}
', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('date', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "fields": {
    "date": {
      "type": "date"
    }
  }
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('timestamp without time zone', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "fields": {
    "date": {
      "type": "date"
    }
  }
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('timestamp with time zone', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "fields": {
    "date": {
      "type": "date"
    }
  }
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('json', '{
  "type": "nested",
  "include_in_parent": true
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('jsonb', '{
  "type": "nested",
  "include_in_parent": true
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('inet', '{
  "type": "ip",
  "copy_to": "zdb_all"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('zdb.phrase', '{
  "type": "text",
  "copy_to": "zdb_all",
  "fielddata": true,
  "analyzer": "phrase"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('zdb.phrase_array', '{
  "type": "text",
  "copy_to": "zdb_all",
  "fielddata": true,
  "analyzer": "phrase"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('zdb.fulltext', '{
  "type": "text",
  "fielddata": true,
  "analyzer": "fulltext"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('zdb.fulltext_with_shingles', '{
  "type": "text",
  "fielddata": true,
  "analyzer": "fulltext_with_shingles",
  "search_analyzer": "fulltext_with_shingles_search"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('point', '{
  "type": "geo_point"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('uuid', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "ignore_above": 10922,
  "normalizer": "lowercase"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('tsvector', '{
  "type": "text",
  "copy_to": "zdb_all",
  "fielddata": true,
  "index_prefixes": { },
  "analyzer": "zdb_standard"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
--
-- emoji analyzer support
--

INSERT INTO zdb.tokenizers(name, definition, is_default)
VALUES ('emoji', '{
  "type": "pattern",
  "pattern": "([\\ud83c\\udf00-\\ud83d\\ude4f]|[\\ud83d\\ude80-\\ud83d\\udeff])",
  "group": 1
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.analyzers(name, definition, is_default)
VALUES ('emoji', '{
  "tokenizer": "emoji"
}', true) ON CONFLICT (name) DO UPDATE SET definition = excluded.definition;
DROP FUNCTION IF EXISTS zdb.ctid(as_u64 pg_catalog.int8) CASCADE;
CREATE OR REPLACE FUNCTION zdb.ctid(as_u64 pg_catalog.int8) RETURNS tid immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'ctid_wrapper';
DROP FUNCTION IF EXISTS zdb.to_queries_dsl(queries zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION zdb.to_queries_dsl(queries zdbquery[]) RETURNS json[] immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'to_queries_dsl_wrapper';
DROP FUNCTION IF EXISTS zdb.zdbquery_from_text(input text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.zdbquery_from_text(input text) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_from_text_wrapper';
DROP FUNCTION IF EXISTS zdb.zdbquery_from_jsonb(input jsonb) CASCADE;
CREATE OR REPLACE FUNCTION zdb.zdbquery_from_jsonb(input jsonb) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_from_jsonb_wrapper';
DROP FUNCTION IF EXISTS zdb.zdbquery_to_jsonb(input zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.zdbquery_to_jsonb(input zdbquery) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_to_jsonb_wrapper';
DROP FUNCTION IF EXISTS zdb.point_to_json(point point) CASCADE;
CREATE OR REPLACE FUNCTION zdb.point_to_json(point point) RETURNS json immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'point_to_json_wrapper';
DROP FUNCTION IF EXISTS zdb.point_array_to_json(points point[]) CASCADE;
CREATE OR REPLACE FUNCTION zdb.point_array_to_json(points point[]) RETURNS json immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'point_array_to_json_wrapper';
DROP FUNCTION IF EXISTS dsl.geo_polygon(field text, VARIADIC points point[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.geo_polygon(field text, VARIADIC points point[]) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'geo_polygon_wrapper';
DROP FUNCTION IF EXISTS zdb.anyelement_cmpfunc(element anyelement, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.anyelement_cmpfunc(element anyelement, query zdbquery) RETURNS bool immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'anyelement_cmpfunc_wrapper';
DROP FUNCTION IF EXISTS zdb.restrict(root internal, _operator_oid oid, args internal, var_relid pg_catalog.int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.restrict(root internal, _operator_oid oid, args internal, var_relid pg_catalog.int4) RETURNS pg_catalog.float8 immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'restrict_wrapper';
DROP FUNCTION IF EXISTS zdb.query_tids(index regclass, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.query_tids(index regclass, query zdbquery) RETURNS tid[] immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'query_tids_wrapper';
DROP FUNCTION IF EXISTS dsl.terms(field text, VARIADIC "values" text[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.terms(field text, VARIADIC "values" text[]) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'terms_str_wrapper';
DROP FUNCTION IF EXISTS dsl.prefix(field text, value text) CASCADE;
CREATE OR REPLACE FUNCTION dsl.prefix(field text, value text) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'prefix_wrapper';
DROP FUNCTION IF EXISTS dsl.field_missing(field text) CASCADE;
CREATE OR REPLACE FUNCTION dsl.field_missing(field text) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'field_missing_wrapper';
DROP FUNCTION IF EXISTS dsl.match_all(boost pg_catalog.float4) CASCADE;
CREATE OR REPLACE FUNCTION dsl.match_all(boost pg_catalog.float4 DEFAULT '1.0') RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'match_all_wrapper';
DROP FUNCTION IF EXISTS dsl.match_none() CASCADE;
CREATE OR REPLACE FUNCTION dsl.match_none() RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'match_none_wrapper';
DROP FUNCTION IF EXISTS dsl.terms_array(fieldname text, "array" anyarray) CASCADE;
CREATE OR REPLACE FUNCTION dsl.terms_array(fieldname text, "array" anyarray) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'terms_array_wrapper';
DROP FUNCTION IF EXISTS dsl.terms_lookup(field text, index text, id text, path text, routing text) CASCADE;
CREATE OR REPLACE FUNCTION dsl.terms_lookup(field text, index text, id text, path text, routing text DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'terms_lookup_wrapper';
DROP FUNCTION IF EXISTS dsl."limit"("limit" pg_catalog.int8, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl."limit"("limit" pg_catalog.int8, query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'limit_wrapper';
DROP FUNCTION IF EXISTS dsl."offset"("offset" pg_catalog.int8, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl."offset"("offset" pg_catalog.int8, query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'offset_wrapper';
DROP FUNCTION IF EXISTS dsl.offset_limit("offset" pg_catalog.int8, "limit" pg_catalog.int8, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.offset_limit("offset" pg_catalog.int8, "limit" pg_catalog.int8, query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'offset_limit_wrapper';
DROP FUNCTION IF EXISTS dsl.min_score(min_score pg_catalog.float8, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.min_score(min_score pg_catalog.float8, query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'min_score_wrapper';
DROP FUNCTION IF EXISTS dsl.row_estimate(row_estimate pg_catalog.int8, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.row_estimate(row_estimate pg_catalog.int8, query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'row_estimate_wrapper';
DROP FUNCTION IF EXISTS dsl.sd(field text, "order" sortdirection, mode sortmode) CASCADE;
CREATE OR REPLACE FUNCTION dsl.sd(field text, "order" sortdirection, mode sortmode DEFAULT NULL) RETURNS sortdescriptor immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'sd_wrapper';
DROP FUNCTION IF EXISTS dsl.sort_many(zdbquery zdbquery, VARIADIC sort_descriptors sortdescriptor[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.sort_many(zdbquery zdbquery, VARIADIC sort_descriptors sortdescriptor[]) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'sort_many_wrapper';
DROP FUNCTION IF EXISTS dsl.sort_direct(sort_json json, zdbquery zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.sort_direct(sort_json json, zdbquery zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'sort_direct_wrapper';
DROP FUNCTION IF EXISTS dsl.fuzzy(field text, value text, boost pg_catalog.float4, fuzziness pg_catalog.int4, prefix_length pg_catalog.int8, max_expansions pg_catalog.int8, transpositions bool) CASCADE;
CREATE OR REPLACE FUNCTION dsl.fuzzy(field text, value text, boost pg_catalog.float4 DEFAULT NULL, fuzziness pg_catalog.int4 DEFAULT NULL, prefix_length pg_catalog.int8 DEFAULT NULL, max_expansions pg_catalog.int8 DEFAULT '50', transpositions bool DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'fuzzy_wrapper';
DROP FUNCTION IF EXISTS dsl.span_containing(little zdbquery, big zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.span_containing(little zdbquery, big zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'span_containing_wrapper';
DROP FUNCTION IF EXISTS dsl.span_first(query zdbquery, "end" pg_catalog.int8) CASCADE;
CREATE OR REPLACE FUNCTION dsl.span_first(query zdbquery, "end" pg_catalog.int8) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'span_first_wrapper';
DROP FUNCTION IF EXISTS dsl.span_masking(field text, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.span_masking(field text, query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'span_masking_wrapper';
DROP FUNCTION IF EXISTS dsl.span_multi(query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.span_multi(query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'span_multi_wrapper';
DROP FUNCTION IF EXISTS dsl.span_not(include zdbquery, exclude zdbquery, pre_integer pg_catalog.int8, post_integer pg_catalog.int8, dis_integer pg_catalog.int8) CASCADE;
CREATE OR REPLACE FUNCTION dsl.span_not(include zdbquery, exclude zdbquery, pre_integer pg_catalog.int8 DEFAULT NULL, post_integer pg_catalog.int8 DEFAULT NULL, dis_integer pg_catalog.int8 DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'span_not_wrapper';
DROP FUNCTION IF EXISTS dsl.span_or(VARIADIC clauses zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.span_or(VARIADIC clauses zdbquery[]) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'span_or_wrapper';
DROP FUNCTION IF EXISTS dsl.span_term(field text, value text, boost pg_catalog.float4) CASCADE;
CREATE OR REPLACE FUNCTION dsl.span_term(field text, value text, boost pg_catalog.float4 DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'span_term_wrapper';
DROP FUNCTION IF EXISTS dsl.span_within(little zdbquery, big zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.span_within(little zdbquery, big zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'span_within_wrapper';
DROP FUNCTION IF EXISTS dsl.wildcard(field text, value text, boost pg_catalog.float4) CASCADE;
CREATE OR REPLACE FUNCTION dsl.wildcard(field text, value text, boost pg_catalog.float4 DEFAULT '1.0') RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'wildcard_wrapper';
DROP FUNCTION IF EXISTS dsl.regexp(field text, regexp text, boost pg_catalog.float4, flags regexflags[], max_determinized_states pg_catalog.int4) CASCADE;
CREATE OR REPLACE FUNCTION dsl.regexp(field text, regexp text, boost pg_catalog.float4 DEFAULT NULL, flags regexflags[] DEFAULT NULL, max_determinized_states pg_catalog.int4 DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'regexp_wrapper';
DROP FUNCTION IF EXISTS dsl.script(source text, params json, lang text) CASCADE;
CREATE OR REPLACE FUNCTION dsl.script(source text, params json DEFAULT NULL, lang text DEFAULT 'painless') RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'script_wrapper';
DROP FUNCTION IF EXISTS dsl.constant_score(boost pg_catalog.float4, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.constant_score(boost pg_catalog.float4, query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'constant_score_wrapper';
DROP FUNCTION IF EXISTS dsl.dis_max(queries zdbquery[], boost pg_catalog.float4, tie_breaker pg_catalog.float4) CASCADE;
CREATE OR REPLACE FUNCTION dsl.dis_max(queries zdbquery[], boost pg_catalog.float4 DEFAULT NULL, tie_breaker pg_catalog.float4 DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'dis_max_wrapper';
DROP FUNCTION IF EXISTS dsl.bool(VARIADIC parts boolquerypart[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.bool(VARIADIC parts boolquerypart[]) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'bool_wrapper';
DROP FUNCTION IF EXISTS dsl.should(VARIADIC queries zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.should(VARIADIC queries zdbquery[]) RETURNS boolquerypart immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'should_wrapper';
DROP FUNCTION IF EXISTS dsl.must(VARIADIC queries zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.must(VARIADIC queries zdbquery[]) RETURNS boolquerypart immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'must_wrapper';
DROP FUNCTION IF EXISTS dsl.must_not(VARIADIC queries zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.must_not(VARIADIC queries zdbquery[]) RETURNS boolquerypart immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'must_not_wrapper';
DROP FUNCTION IF EXISTS dsl.filter(VARIADIC queries zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl.filter(VARIADIC queries zdbquery[]) RETURNS boolquerypart immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'filter_wrapper';
DROP FUNCTION IF EXISTS dsl.binary_and(a zdbquery, b zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.binary_and(a zdbquery, b zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'binary_and_wrapper';
DROP FUNCTION IF EXISTS dsl."and"(VARIADIC queries zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl."and"(VARIADIC queries zdbquery[]) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'and_wrapper';
DROP FUNCTION IF EXISTS dsl."not"(VARIADIC queries zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION dsl."not"(VARIADIC queries zdbquery[]) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'not_wrapper';
DROP FUNCTION IF EXISTS dsl.noteq(query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.noteq(query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'noteq_wrapper';
DROP FUNCTION IF EXISTS dsl.match(field text, query text, boost pg_catalog.float4, analyzer text, minimum_should_match pg_catalog.int4, lenient bool, fuzziness pg_catalog.int4, fuzzy_rewrite text, fuzzy_transpositions bool, prefix_length pg_catalog.int4, cutoff_frequency pg_catalog.float4, auto_generate_synonyms_phrase_query bool, zero_terms_query zerotermsquery, operator operator) CASCADE;
CREATE OR REPLACE FUNCTION dsl.match(field text, query text, boost pg_catalog.float4 DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match pg_catalog.int4 DEFAULT NULL, lenient bool DEFAULT NULL, fuzziness pg_catalog.int4 DEFAULT NULL, fuzzy_rewrite text DEFAULT NULL, fuzzy_transpositions bool DEFAULT NULL, prefix_length pg_catalog.int4 DEFAULT NULL, cutoff_frequency pg_catalog.float4 DEFAULT NULL, auto_generate_synonyms_phrase_query bool DEFAULT NULL, zero_terms_query zerotermsquery DEFAULT NULL, operator operator DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'match_wrapper_wrapper';
DROP FUNCTION IF EXISTS dsl.multi_match(fields text[], query text, boost pg_catalog.float4, analyzer text, minimum_should_match pg_catalog.int4, lenient bool, fuzziness pg_catalog.int4, fuzzy_rewrite text, fuzzy_transpositions bool, prefix_length pg_catalog.int4, cutoff_frequency pg_catalog.float4, auto_generate_synonyms_phrase_query bool, zero_terms_query zerotermsquery, operator operator, match_type matchtype) CASCADE;
CREATE OR REPLACE FUNCTION dsl.multi_match(fields text[], query text, boost pg_catalog.float4 DEFAULT NULL, analyzer text DEFAULT NULL, minimum_should_match pg_catalog.int4 DEFAULT NULL, lenient bool DEFAULT NULL, fuzziness pg_catalog.int4 DEFAULT NULL, fuzzy_rewrite text DEFAULT NULL, fuzzy_transpositions bool DEFAULT NULL, prefix_length pg_catalog.int4 DEFAULT NULL, cutoff_frequency pg_catalog.float4 DEFAULT NULL, auto_generate_synonyms_phrase_query bool DEFAULT NULL, zero_terms_query zerotermsquery DEFAULT NULL, operator operator DEFAULT NULL, match_type matchtype DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'multi_match_wrapper';
DROP FUNCTION IF EXISTS dsl.match_phrase_prefix(field text, query text, boost pg_catalog.float4, slop pg_catalog.int4, analyzer text, maxexpansion pg_catalog.int4, zero_terms_query zerotermsquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.match_phrase_prefix(field text, query text, boost pg_catalog.float4 DEFAULT NULL, slop pg_catalog.int4 DEFAULT NULL, analyzer text DEFAULT NULL, maxexpansion pg_catalog.int4 DEFAULT NULL, zero_terms_query zerotermsquery DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'match_phrase_prefix_wrapper';
DROP FUNCTION IF EXISTS dsl.nested(path text, query zdbquery, score_mode scoremode, ignore_unmapped bool) CASCADE;
CREATE OR REPLACE FUNCTION dsl.nested(path text, query zdbquery, score_mode scoremode DEFAULT 'avg', ignore_unmapped bool DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'nested_wrapper';
DROP FUNCTION IF EXISTS dsl.datetime_range(field text, lt date, gt date, lte date, gte date, boost pg_catalog.float4, relation relation) CASCADE;
CREATE OR REPLACE FUNCTION dsl.datetime_range(field text, lt date DEFAULT NULL, gt date DEFAULT NULL, lte date DEFAULT NULL, gte date DEFAULT NULL, boost pg_catalog.float4 DEFAULT NULL, relation relation DEFAULT 'intersects') RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'datetime_range_date_wrapper';
DROP FUNCTION IF EXISTS dsl.query_string(query text, default_field text, allow_leading_wildcard bool, analyze_wildcard bool, analyzer text, auto_generate_synonyms_phrase_query bool, boost pg_catalog.float4, default_operator querystringdefaultoperator, enable_position_increments bool, fields text[], fuzziness pg_catalog.int4, fuzzy_max_expansions pg_catalog.int8, fuzzy_transpositions bool, fuzzy_prefix_length pg_catalog.int8, lenient bool, max_determinized_states pg_catalog.int8, minimum_should_match pg_catalog.int4, quote_analyzer text, phrase_slop pg_catalog.int8, quote_field_suffix text, time_zone text) CASCADE;
CREATE OR REPLACE FUNCTION dsl.query_string(query text, default_field text DEFAULT NULL, allow_leading_wildcard bool DEFAULT NULL, analyze_wildcard bool DEFAULT NULL, analyzer text DEFAULT NULL, auto_generate_synonyms_phrase_query bool DEFAULT NULL, boost pg_catalog.float4 DEFAULT NULL, default_operator querystringdefaultoperator DEFAULT NULL, enable_position_increments bool DEFAULT NULL, fields text[] DEFAULT NULL, fuzziness pg_catalog.int4 DEFAULT NULL, fuzzy_max_expansions pg_catalog.int8 DEFAULT NULL, fuzzy_transpositions bool DEFAULT NULL, fuzzy_prefix_length pg_catalog.int8 DEFAULT NULL, lenient bool DEFAULT NULL, max_determinized_states pg_catalog.int8 DEFAULT NULL, minimum_should_match pg_catalog.int4 DEFAULT NULL, quote_analyzer text DEFAULT NULL, phrase_slop pg_catalog.int8 DEFAULT NULL, quote_field_suffix text DEFAULT NULL, time_zone text DEFAULT NULL) RETURNS zdbquery immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'query_string_wrapper';
DROP FUNCTION IF EXISTS zdb.profile_query(index regclass, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.profile_query(index regclass, query zdbquery) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'profile_query_wrapper';
DROP FUNCTION IF EXISTS zdb.terms(index regclass, field_name text, query zdbquery, size_limit pg_catalog.int4, order_by termsorderby) CASCADE;
CREATE OR REPLACE FUNCTION zdb.terms(index regclass, field_name text, query zdbquery, size_limit pg_catalog.int4 DEFAULT '2147483647', order_by termsorderby DEFAULT NULL) RETURNS TABLE(term text, doc_count pg_catalog.int8) immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'terms_wrapper';
DROP FUNCTION IF EXISTS zdb.tally(index regclass, field_name text, stem text, query zdbquery, size_limit pg_catalog.int4, order_by termsorderby, shard_size pg_catalog.int4, count_nulls bool) CASCADE;
CREATE OR REPLACE FUNCTION zdb.tally(index regclass, field_name text, stem text, query zdbquery, size_limit pg_catalog.int4 DEFAULT '2147483647', order_by termsorderby DEFAULT NULL, shard_size pg_catalog.int4 DEFAULT '2147483647', count_nulls bool DEFAULT 'true') RETURNS TABLE(term text, count pg_catalog.int8) immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'tally_not_nested_wrapper';
DROP FUNCTION IF EXISTS zdb.significant_terms(index regclass, field text, query zdbquery, include text, size_limit pg_catalog.int4, min_doc_count pg_catalog.int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.significant_terms(index regclass, field text, query zdbquery, include text DEFAULT '.*', size_limit pg_catalog.int4 DEFAULT '2147483647', min_doc_count pg_catalog.int4 DEFAULT '3') RETURNS TABLE(term text, doc_count pg_catalog.int8, score pg_catalog.float4, bg_count pg_catalog.int8) immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'significant_terms_wrapper';
DROP FUNCTION IF EXISTS zdb.histogram(index regclass, field text, query zdbquery, "interval" pg_catalog.float8, min_doc_count pg_catalog.int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.histogram(index regclass, field text, query zdbquery, "interval" pg_catalog.float8, min_doc_count pg_catalog.int4 DEFAULT '0') RETURNS TABLE(term pg_catalog."numeric", doc_count pg_catalog.int8) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'histogram_wrapper';
DROP FUNCTION IF EXISTS zdb.arbitrary_agg(index regclass, query zdbquery, json jsonb) CASCADE;
CREATE OR REPLACE FUNCTION zdb.arbitrary_agg(index regclass, query zdbquery, json jsonb) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'arbitrary_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.determine_index(relation regclass) CASCADE;
CREATE OR REPLACE FUNCTION zdb.determine_index(relation regclass) RETURNS regclass STRICT volatile PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'determine_index_wrapper';
DROP FUNCTION IF EXISTS zdb.index_links(relation regclass) CASCADE;
CREATE OR REPLACE FUNCTION zdb.index_links(relation regclass) RETURNS text[] STRICT volatile PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'index_links_wrapper';
DROP FUNCTION IF EXISTS zdb.index_name(index_relation regclass) CASCADE;
CREATE OR REPLACE FUNCTION zdb.index_name(index_relation regclass) RETURNS text STRICT volatile PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'index_name_wrapper';
DROP FUNCTION IF EXISTS zdb.index_alias(index_relation regclass) CASCADE;
CREATE OR REPLACE FUNCTION zdb.index_alias(index_relation regclass) RETURNS text STRICT volatile PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'index_alias_wrapper';
DROP FUNCTION IF EXISTS zdb.index_url(index_relation regclass) CASCADE;
CREATE OR REPLACE FUNCTION zdb.index_url(index_relation regclass) RETURNS text STRICT volatile PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'index_url_wrapper';
DROP FUNCTION IF EXISTS zdb.index_type_name(index_relation regclass) CASCADE;
CREATE OR REPLACE FUNCTION zdb.index_type_name(index_relation regclass) RETURNS text STRICT volatile PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'index_type_name_wrapper';
DROP FUNCTION IF EXISTS zdb.index_mapping(index_relation regclass) CASCADE;
CREATE OR REPLACE FUNCTION zdb.index_mapping(index_relation regclass) RETURNS jsonb STRICT volatile PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'index_mapping_wrapper';
DROP FUNCTION IF EXISTS zdb.field_mapping(index_relation regclass, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.field_mapping(index_relation regclass, field text) RETURNS jsonb STRICT volatile PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'field_mapping_wrapper';
DROP FUNCTION IF EXISTS zdb.count(index regclass, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.count(index regclass, query zdbquery) RETURNS pg_catalog.int8 immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'count_wrapper';
DROP FUNCTION IF EXISTS zdb.raw_count(index regclass, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.raw_count(index regclass, query zdbquery) RETURNS pg_catalog.int8 immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'raw_count_wrapper';
DROP FUNCTION IF EXISTS zdb.significant_terms_two_level(index regclass, field_first text, field_second text, query zdbquery, size_limit pg_catalog.int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.significant_terms_two_level(index regclass, field_first text, field_second text, query zdbquery, size_limit pg_catalog.int4 DEFAULT '2147483647') RETURNS TABLE(term_one text, term_two text, doc_count pg_catalog.int8, score pg_catalog.float8, bg_count pg_catalog.int8, doc_count_error_upper_bound pg_catalog.int8, sum_other_doc_count pg_catalog.int8) immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'significant_terms_two_level_wrapper';
DROP FUNCTION IF EXISTS zdb.matrix_stats(index regclass, fields text[], query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.matrix_stats(index regclass, fields text[], query zdbquery) RETURNS TABLE(term text, count pg_catalog.int8, mean pg_catalog."numeric", variance pg_catalog."numeric", skewness pg_catalog."numeric", kurtosis pg_catalog."numeric", covariance json, correlation json) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'matrix_stats_wrapper';
DROP FUNCTION IF EXISTS zdb.avg(index regclass, field text, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.avg(index regclass, field text, query zdbquery) RETURNS pg_catalog."numeric" immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'avg_wrapper';
DROP FUNCTION IF EXISTS zdb.cardinality(index regclass, field text, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.cardinality(index regclass, field text, query zdbquery) RETURNS pg_catalog."numeric" immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'cardinality_wrapper';
DROP FUNCTION IF EXISTS zdb.max(index regclass, field text, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.max(index regclass, field text, query zdbquery) RETURNS pg_catalog."numeric" immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'max_wrapper';
DROP FUNCTION IF EXISTS zdb.missing(index regclass, field text, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.missing(index regclass, field text, query zdbquery) RETURNS pg_catalog."numeric" immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'missing_wrapper';
DROP FUNCTION IF EXISTS zdb.value_count(index regclass, field text, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.value_count(index regclass, field text, query zdbquery) RETURNS pg_catalog."numeric" immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'value_count_wrapper';
DROP FUNCTION IF EXISTS zdb.stats(index regclass, field text, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.stats(index regclass, field text, query zdbquery) RETURNS TABLE(count pg_catalog.int8, min pg_catalog."numeric", max pg_catalog."numeric", avg pg_catalog."numeric", sum pg_catalog."numeric") immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'stats_wrapper';
DROP FUNCTION IF EXISTS zdb.range(index regclass, field text, query zdbquery, range_array json) CASCADE;
CREATE OR REPLACE FUNCTION zdb.range(index regclass, field text, query zdbquery, range_array json) RETURNS TABLE(key text, "from" pg_catalog."numeric", "to" pg_catalog."numeric", doc_count pg_catalog.int8) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'range_wrapper';
DROP FUNCTION IF EXISTS zdb.date_range(index regclass, field text, query zdbquery, date_range_array json) CASCADE;
CREATE OR REPLACE FUNCTION zdb.date_range(index regclass, field text, query zdbquery, date_range_array json) RETURNS TABLE(key text, "from" pg_catalog."numeric", from_as_string text, "to" pg_catalog."numeric", to_as_string text, doc_count pg_catalog.int8) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'date_range_wrapper';
DROP FUNCTION IF EXISTS zdb.extended_stats(index regclass, field text, query zdbquery, sigma pg_catalog.int8) CASCADE;
CREATE OR REPLACE FUNCTION zdb.extended_stats(index regclass, field text, query zdbquery, sigma pg_catalog.int8 DEFAULT '0') RETURNS TABLE(count pg_catalog.int8, min pg_catalog."numeric", max pg_catalog."numeric", avg pg_catalog."numeric", sum pg_catalog."numeric", sum_of_squares pg_catalog."numeric", variance pg_catalog."numeric", std_deviation pg_catalog."numeric", upper pg_catalog."numeric", lower pg_catalog."numeric") immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'extended_stats_wrapper';
DROP FUNCTION IF EXISTS zdb.top_hits(index regclass, fields text[], query zdbquery, size_limit pg_catalog.int8) CASCADE;
CREATE OR REPLACE FUNCTION zdb.top_hits(index regclass, fields text[], query zdbquery, size_limit pg_catalog.int8) RETURNS TABLE(id tid, score pg_catalog.float8, source json) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'top_hits_wrapper';
DROP FUNCTION IF EXISTS zdb.top_hits_with_id(index regclass, fields text[], query zdbquery, size_limit pg_catalog.int8) CASCADE;
CREATE OR REPLACE FUNCTION zdb.top_hits_with_id(index regclass, fields text[], query zdbquery, size_limit pg_catalog.int8) RETURNS TABLE(id text, score pg_catalog.float8, source json) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'top_hits_with_id_wrapper';
DROP FUNCTION IF EXISTS zdb.ip_range(index regclass, field text, query zdbquery, range_array json) CASCADE;
CREATE OR REPLACE FUNCTION zdb.ip_range(index regclass, field text, query zdbquery, range_array json) RETURNS TABLE(key text, "from" inet, "to" inet, doc_count pg_catalog.int8) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'ip_range_wrapper';
DROP FUNCTION IF EXISTS zdb.query(index regclass, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.query(index regclass, query zdbquery) RETURNS SETOF tid immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'query_wrapper';
DROP FUNCTION IF EXISTS zdb.analyze_text(index regclass, analyzer text, text text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.analyze_text(index regclass, analyzer text, text text) RETURNS TABLE(type text, token text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'analyze_text_wrapper';
DROP FUNCTION IF EXISTS zdb.analyze_with_field(index regclass, field text, text text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.analyze_with_field(index regclass, field text, text text) RETURNS TABLE(type text, token text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'analyze_with_field_wrapper';
DROP FUNCTION IF EXISTS zdb.analyze_custom(index regclass, field text, text text, tokenizer text, normalizer text, filter text[], char_filter text[]) CASCADE;
CREATE OR REPLACE FUNCTION zdb.analyze_custom(index regclass, field text DEFAULT NULL, text text DEFAULT NULL, tokenizer text DEFAULT NULL, normalizer text DEFAULT NULL, filter text[] DEFAULT NULL, char_filter text[] DEFAULT NULL) RETURNS TABLE(type text, token text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8) immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'analyze_custom_wrapper';
DROP FUNCTION IF EXISTS zdb.score(ctid tid) CASCADE;
CREATE OR REPLACE FUNCTION zdb.score(ctid tid) RETURNS pg_catalog.float8 immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'score_wrapper';
DROP FUNCTION IF EXISTS zdb.want_scores(query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.want_scores(query zdbquery) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'want_scores_wrapper';
DROP FUNCTION IF EXISTS zdb.highlight_phrase(index regclass, field_name text, text text, tokens_to_highlight text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight_phrase(index regclass, field_name text, text text, tokens_to_highlight text) RETURNS TABLE(field_name text, term text, type text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8) STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_phrase_wrapper';
DROP FUNCTION IF EXISTS zdb.highlight_fuzzy(index regclass, field_name text, text text, token_to_highlight text, prefix pg_catalog.int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight_fuzzy(index regclass, field_name text, text text, token_to_highlight text, prefix pg_catalog.int4) RETURNS TABLE(field_name text, term text, type text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8) STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_fuzzy_wrapper';
DROP FUNCTION IF EXISTS zdb.highlight_proximity(index regclass, field_name text, text text, prox_clause proximitypart[]) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight_proximity(index regclass, field_name text, text text, prox_clause proximitypart[]) RETURNS TABLE(field_name text, term text, type text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_proximity_wrapper';
DROP FUNCTION IF EXISTS zdb.cat_request(index regclass, endpoint text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.cat_request(index regclass, endpoint text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'cat_request_wrapper';
DROP FUNCTION IF EXISTS zdb.highlight(highlight_type highlighttype, require_field_match bool, number_of_fragments pg_catalog.int4, highlight_query zdbquery, pre_tags text[], post_tags text[], tags_schema text, no_match_size pg_catalog.int4, fragmenter fragmentertype, fragment_size pg_catalog.int4, fragment_offset pg_catalog.int4, force_source bool, encoder encodertype, boundary_scanner_locale text, boundary_scan_max pg_catalog.int4, boundary_chars text, phrase_limit pg_catalog.int4, matched_fields bool, "order" text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight(highlight_type highlighttype DEFAULT NULL, require_field_match bool DEFAULT 'false', number_of_fragments pg_catalog.int4 DEFAULT NULL, highlight_query zdbquery DEFAULT NULL, pre_tags text[] DEFAULT NULL, post_tags text[] DEFAULT NULL, tags_schema text DEFAULT NULL, no_match_size pg_catalog.int4 DEFAULT NULL, fragmenter fragmentertype DEFAULT NULL, fragment_size pg_catalog.int4 DEFAULT NULL, fragment_offset pg_catalog.int4 DEFAULT NULL, force_source bool DEFAULT 'true', encoder encodertype DEFAULT NULL, boundary_scanner_locale text DEFAULT NULL, boundary_scan_max pg_catalog.int4 DEFAULT NULL, boundary_chars text DEFAULT NULL, phrase_limit pg_catalog.int4 DEFAULT NULL, matched_fields bool DEFAULT NULL, "order" text DEFAULT NULL) RETURNS json immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_wrapper';
DROP FUNCTION IF EXISTS zdb.want_highlight(query zdbquery, field text, highlight_definition json) CASCADE;
CREATE OR REPLACE FUNCTION zdb.want_highlight(query zdbquery, field text, highlight_definition json DEFAULT zdb.highlight()) RETURNS zdbquery immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'want_highlight_wrapper';
DROP FUNCTION IF EXISTS zdb.dump_query(index regclass, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.dump_query(index regclass, query zdbquery) RETURNS text immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'dump_query_wrapper';
DROP FUNCTION IF EXISTS zdb.debug_query(index regclass, query text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.debug_query(index regclass, query text) RETURNS TABLE(normalized_query text, used_fields text[], ast text) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'debug_query_wrapper';
DROP FUNCTION IF EXISTS zdb.suggest_terms(index regclass, field_name text, suggest text, query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION zdb.suggest_terms(index regclass, field_name text, suggest text, query zdbquery) RETURNS TABLE(term text, "offset" pg_catalog.int8, length pg_catalog.int8, suggestion text, score pg_catalog.float8, frequency pg_catalog.int8) immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'suggest_terms_wrapper';
DROP FUNCTION IF EXISTS zdb.sum_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.sum_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'sum_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.stats_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.stats_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'stats_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.extended_stats_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.extended_stats_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'extended_stats_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.matrix_stats_agg(aggregate_name text, field text[]) CASCADE;
CREATE OR REPLACE FUNCTION zdb.matrix_stats_agg(aggregate_name text, field text[]) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'matrix_stats_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.geo_bounds_agg(aggregate_name text, field text, wrap_longitude bool) CASCADE;
CREATE OR REPLACE FUNCTION zdb.geo_bounds_agg(aggregate_name text, field text, wrap_longitude bool) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'geo_bounds_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.value_count_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.value_count_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'value_count_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.boxplot_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.boxplot_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'boxplot_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.geo_centroid_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.geo_centroid_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'geo_centroid_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.median_absolute_deviation_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.median_absolute_deviation_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'median_absolute_deviation_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.percentiles_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.percentiles_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.string_stats_agg(aggregate_name text, field text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.string_stats_agg(aggregate_name text, field text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'string_stats_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.weighted_avg_agg(aggregate_name text, field_value text, field_weight text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.weighted_avg_agg(aggregate_name text, field_value text, field_weight text) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'weighted_avg_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.top_metrics_agg(aggregate_name text, metric_field text, sort_type sortdescriptor) CASCADE;
CREATE OR REPLACE FUNCTION zdb.top_metrics_agg(aggregate_name text, metric_field text, sort_type sortdescriptor) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'top_metric_sort_desc_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.t_test_agg(aggregate_name text, fields text[], t_type ttesttype) CASCADE;
CREATE OR REPLACE FUNCTION zdb.t_test_agg(aggregate_name text, fields text[], t_type ttesttype) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 't_test_fields_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.filter_agg(index regclass, aggregate_name text, filter zdbquery, children jsonb[]) CASCADE;
CREATE OR REPLACE FUNCTION zdb.filter_agg(index regclass, aggregate_name text, filter zdbquery, children jsonb[] DEFAULT NULL) RETURNS jsonb immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'filter_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.filters_agg(index regclass, aggregate_name text, labels text[], filters zdbquery[]) CASCADE;
CREATE OR REPLACE FUNCTION zdb.filters_agg(index regclass, aggregate_name text, labels text[], filters zdbquery[]) RETURNS jsonb immutable STRICT PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'filters_agg_wrapper';

