DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '@DEFAULT_VERSION@ (@GIT_HASH@)'
$$;
DROP FUNCTION IF EXISTS zdb.restrict(root internal, _operator_oid oid, args internal, var_relid int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.restrict(root internal, _operator_oid oid, args internal, var_relid int4) RETURNS float8 AS 'MODULE_PATHNAME', 'restrict_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE;
DROP FUNCTION IF EXISTS zdb.highlight_term(index regclass, field_name text, text text, token_to_highlight text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight_term(index regclass, field_name text, text text, token_to_highlight text) RETURNS TABLE(field_name text, term text, type text, "position" int4, start_offset int8, end_offset int8) AS 'MODULE_PATHNAME', 'highlight_term_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS zdb.highlight_wildcard(index regclass, field_name text, text text, token_to_highlight text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight_wildcard(index regclass, field_name text, text text, token_to_highlight text) RETURNS TABLE(field_name text, term text, type text, "position" int4, start_offset int8, end_offset int8) AS 'MODULE_PATHNAME', 'highlight_wildcard_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS zdb.highlight_phrase(index regclass, field_name text, text text, tokens_to_highlight text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight_phrase(index regclass, field_name text, text text, tokens_to_highlight text) RETURNS TABLE(field_name text, term text, type text, "position" int4, start_offset int8, end_offset int8) AS 'MODULE_PATHNAME', 'highlight_phrase_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS zdb.highlight_fuzzy(index regclass, field_name text, text text, token_to_highlight text, prefix int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight_fuzzy(index regclass, field_name text, text text, token_to_highlight text, prefix int4) RETURNS TABLE(field_name text, term text, type text, "position" int4, start_offset int8, end_offset int8) AS 'MODULE_PATHNAME', 'highlight_fuzzy_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS zdb.highlight_regex(index regclass, field_name text, text text, token_to_highlight text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.highlight_regex(index regclass, field_name text, text text, token_to_highlight text) RETURNS TABLE(field_name text, term text, type text, "position" int4, start_offset int8, end_offset int8) AS 'MODULE_PATHNAME', 'highlight_regex_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS zdb.t_test_agg(aggregate_name text, fields text[], t_type ttesttype) CASCADE;
CREATE OR REPLACE FUNCTION zdb.t_test_agg(aggregate_name text, fields text[], t_type zdb.ttesttype) RETURNS jsonb AS 'MODULE_PATHNAME', 't_test_fields_agg_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS zdb.diversified_sampler_agg(aggregate_name text, shard_size int8, max_docs_per_value int8, execution_hint executionhint, children jsonb[]) CASCADE;
CREATE OR REPLACE FUNCTION zdb.diversified_sampler_agg(aggregate_name text, shard_size int8 DEFAULT NULL, max_docs_per_value int8 DEFAULT NULL, execution_hint zdb.executionhint DEFAULT NULL, children jsonb[] DEFAULT NULL) RETURNS jsonb AS 'MODULE_PATHNAME', 'diversified_sampler_agg_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE;
DROP FUNCTION IF EXISTS zdb.auto_date_histogram_agg(aggregate_name text, field text, buckets int8, format text, minimum_interval intervals, missing text) CASCADE;
CREATE OR REPLACE FUNCTION zdb.auto_date_histogram_agg(aggregate_name text, field text, buckets int8, format text DEFAULT NULL, minimum_interval zdb.intervals DEFAULT NULL, missing text DEFAULT NULL) RETURNS jsonb AS 'MODULE_PATHNAME', 'auto_date_histogram_agg_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE;
DROP FUNCTION IF EXISTS zdb.t_test_agg(aggregate_name text, fields text[], queries zdbquery[], t_type ttesttype) CASCADE;
CREATE OR REPLACE FUNCTION zdb.t_test_agg(aggregate_name text, fields text[], queries zdbquery[], t_type zdb.ttesttype) RETURNS jsonb AS 'MODULE_PATHNAME', 't_test_fields_queries_agg_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS dsl.link_options(options text[], query zdbquery) CASCADE;
CREATE OR REPLACE FUNCTION dsl.link_options(options text[], query zdbquery) RETURNS zdbquery AS 'MODULE_PATHNAME', 'link_options_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('text', '{
  "type": "text",
  "copy_to": "zdb_all",
  "fielddata": true,
  "analyzer": "zdb_standard"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('tsvector', '{
  "type": "text",
  "copy_to": "zdb_all",
  "fielddata": true,
  "analyzer": "zdb_standard"
}', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;

