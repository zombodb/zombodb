CREATE OR REPLACE FUNCTION zdb_tally(table_name regclass, fieldname text, is_nested boolean, stem text, query text, max_terms bigint, sort_order zdb_tally_order) RETURNS SETOF zdb_tally_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
  json_data json;
  type_oid oid;
  missing bigint;
  nested boolean;
  data_type text;
  buckets json;
BEGIN
  type_oid := zdb_determine_index(table_name);

  SELECT typname FROM pg_type WHERE oid = (SELECT atttypid FROM pg_attribute WHERE attrelid = table_name AND attname = fieldname) INTO data_type;

  json_data := zdb_internal_tally(type_oid, CASE WHEN is_nested THEN format('#nested(%s)', fieldname) ELSE fieldname END, stem, query, max_terms, sort_order::text);
  nested := (json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets') IS NOT NULL;

  IF nested THEN
    missing := (json_data->'aggregations'->'nested'->'filter'->'missing'->>'doc_count')::bigint;
    buckets := json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets';
  ELSE
    missing := (json_data->'aggregations'->'missing'->>'doc_count')::bigint;
    buckets := json_data->'aggregations'->fieldname->'buckets';
  END IF;

  IF missing IS NULL OR missing = 0 THEN
    RETURN QUERY (
      SELECT
        coalesce(upper((x->>'key_as_string')::text), upper((x->>'key')::text)),
        (x->>'doc_count')::int8
      FROM json_array_elements(buckets) x
    );
  ELSE
    RETURN QUERY (
      SELECT * FROM (SELECT NULL::text, missing LIMIT missing) x
      UNION ALL
      SELECT
        coalesce(upper((x->>'key_as_string')::text), upper((x->>'key')::text)),
        (x->>'doc_count')::int8
      FROM json_array_elements(buckets) x
    );
  END IF;
END;
$$;

--
-- filter/analyzer/mapping support
--

CREATE TABLE zdb_filters (
  name text NOT NULL PRIMARY KEY,
  definition json NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb_char_filters (
  name text NOT NULL PRIMARY KEY,
  definition json NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb_analyzers (
  name text NOT NULL PRIMARY KEY,
  definition json NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb_mappings (
  table_name regclass NOT NULL,
  field_name name NOT NULL,
  definition json NOT NULL,
  PRIMARY KEY (table_name, field_name)
);

SELECT pg_catalog.pg_extension_config_dump('zdb_filters', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb_char_filters', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb_analyzers', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb_mappings', '');

CREATE OR REPLACE FUNCTION zdb_define_filter(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
DELETE FROM zdb_filters WHERE name = $1;
INSERT INTO zdb_filters(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb_define_char_filter(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
DELETE FROM zdb_char_filters WHERE name = $1;
INSERT INTO zdb_char_filters(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb_define_analyzer(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
DELETE FROM zdb_analyzers WHERE name = $1;
INSERT INTO zdb_analyzers(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb_define_mapping(table_name regclass, field_name name, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
DELETE FROM zdb_mappings WHERE table_name = $1 AND field_name = $2;
INSERT INTO zdb_mappings(table_name, field_name, definition) VALUES ($1, $2, $3);
$$;

INSERT INTO zdb_filters(name, definition, is_default) VALUES (
  'zdb_truncate_32000', '{
          "type": "truncate",
          "length": 32000
        }', true);

INSERT INTO zdb_analyzers(name, definition, is_default) VALUES (
  'exact', '{
          "tokenizer": "keyword",
          "filter": ["trim", "zdb_truncate_32000", "lowercase"]
        }', true);
INSERT INTO zdb_analyzers(name, definition, is_default) VALUES (
  'phrase', '{
          "tokenizer": "standard",
          "filter": ["lowercase"]
        }', true);
INSERT INTO zdb_analyzers(name, definition, is_default) VALUES (
  'fulltext', '{
          "tokenizer": "standard",
          "filter": ["lowercase"]
        }', true);

CREATE DOMAIN arabic AS text;
CREATE DOMAIN armenian AS text;
CREATE DOMAIN basque AS text;
CREATE DOMAIN brazilian AS text;
CREATE DOMAIN bulgarian AS text;
CREATE DOMAIN catalan AS text;
CREATE DOMAIN chinese AS text;
CREATE DOMAIN cjk AS text;
CREATE DOMAIN czech AS text;
CREATE DOMAIN danish AS text;
CREATE DOMAIN dutch AS text;
CREATE DOMAIN english AS text;
CREATE DOMAIN finnish AS text;
CREATE DOMAIN french AS text;
CREATE DOMAIN galician AS text;
CREATE DOMAIN german AS text;
CREATE DOMAIN greek AS text;
CREATE DOMAIN hindi AS text;
CREATE DOMAIN hungarian AS text;
CREATE DOMAIN indonesian AS text;
CREATE DOMAIN irish AS text;
CREATE DOMAIN italian AS text;
CREATE DOMAIN latvian AS text;
CREATE DOMAIN norwegian AS text;
CREATE DOMAIN persian AS text;
CREATE DOMAIN portuguese AS text;
CREATE DOMAIN romanian AS text;
CREATE DOMAIN russian AS text;
CREATE DOMAIN sorani AS text;
CREATE DOMAIN spanish AS text;
CREATE DOMAIN swedish AS text;
CREATE DOMAIN turkish AS text;
CREATE DOMAIN thai AS text;

CREATE TYPE zdb_analyze_text_response AS (token text, start_offset integer, end_offset integer, type text, position integer);
CREATE OR REPLACE FUNCTION zdb_internal_analyze_text(index_name regclass, analyzer_name text, data text) RETURNS json IMMUTABLE STRICT LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_analyze_text(index_name regclass, analyzer_name text, data text) RETURNS SETOF zdb_analyze_text_response IMMUTABLE STRICT LANGUAGE plpgsql AS $$
DECLARE
  results json;
BEGIN
  results := zdb_internal_analyze_text(index_name, analyzer_name, data);
  RETURN QUERY SELECT (value->>'token')::text,
                 (value->>'start_offset')::integer,
                 (value->>'end_offset')::integer,
                 (value->>'type')::text,
                 (value->>'position')::integer
               FROM json_array_elements(results->'tokens');
END;
$$;