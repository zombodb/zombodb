DROP FUNCTION IF EXISTS zdb.schema_version();
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.4 (759bfe45fe5b8ee13cdb0100dc49eff9e6dd116a)'
$$;
-- src/mapping/mod.rs:9
-- zombodb::mapping::reapply_mapping
CREATE  FUNCTION zdb."reapply_mapping"(
	"index_relation" regclass /* pgx::rel::PgRelation */
) RETURNS bool /* bool */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'reapply_mapping_wrapper';

ALTER FUNCTION pg_catalog.proximitypart_in STRICT;
ALTER FUNCTION pg_catalog.sortdescriptor_in STRICT;
ALTER FUNCTION pg_catalog.sortdescriptoroptions_in STRICT;
ALTER FUNCTION pg_catalog.zdbquery_in STRICT;
ALTER FUNCTION pg_catalog.boolquerypart_in STRICT;


DROP FUNCTION IF EXISTS zdb.highlight_document(index regclass, document jsonb, query_string text);
CREATE OR REPLACE FUNCTION zdb.highlight_document(index regclass, document jsonb, query_string text, dedup_results bool DEFAULT true) RETURNS TABLE(field_name text, array_index pg_catalog.int4, term text, type text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8, query_clause text) AS 'MODULE_PATHNAME', 'highlight_document_jsonb_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS zdb.highlight_document(index regclass, document json, query_string text);
CREATE OR REPLACE FUNCTION zdb.highlight_document(index regclass, document json, query_string text, dedup_results bool DEFAULT true) RETURNS TABLE(field_name text, array_index pg_catalog.int4, term text, type text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8, query_clause text) AS 'MODULE_PATHNAME', 'highlight_document_json_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE STRICT;
DROP FUNCTION IF EXISTS zdb.highlight_proximity(index regclass, field_name text, text text, prox_clause pg_catalog.proximitypart[]);
CREATE OR REPLACE FUNCTION zdb.highlight_proximity(index regclass, field_name text, text text, prox_clause pg_catalog.proximitypart[], dedup_results bool DEFAULT true) RETURNS TABLE(field_name text, term text, type text, "position" pg_catalog.int4, start_offset pg_catalog.int8, end_offset pg_catalog.int8) AS 'MODULE_PATHNAME', 'highlight_proximity_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE;
DROP FUNCTION IF EXISTS dsl.link_options(options text[], query pg_catalog.zdbquery);
CREATE OR REPLACE FUNCTION dsl.link_options(options text[], query pg_catalog.zdbquery) RETURNS pg_catalog.zdbquery AS 'MODULE_PATHNAME', 'link_options_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE;
-- src/highlighting/es_highlighting.rs:164
-- zombodb::highlighting::es_highlighting::highlight_all_fields
-- requires:
--   highlighting::es_highlighting::highlight
CREATE  FUNCTION zdb."highlight_all_fields"(
	"ctid" tid, /* pgx_pg_sys::pg14::ItemPointerData */
	"_highlight_definition" json DEFAULT zdb.highlight() /* pgx::datum::json::Json */
) RETURNS json /* pgx::datum::json::Json */
IMMUTABLE STRICT PARALLEL SAFE 
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'highlight_all_fields_wrapper';

INSERT INTO zdb.type_mappings(type_name, definition, is_default)
VALUES ('time without time zone', '{
  "type": "keyword",
  "copy_to": "zdb_all",
  "fields": {
    "date": {
      "type": "date",
      "format": "HH:mm||HH:mm:ss||HH:mm:ss.S||HH:mm:ss.SS||HH:mm:ss.SSS||HH:mm:ss.SSSS||HH:mm:ss.SSSSS||HH:mm:ss.SSSSSS"
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
      "format": "HH:mmX||HH:mm:ssX||HH:mm:ss.SX||HH:mm:ss.SSX||HH:mm:ss.SSSX||HH:mm:ss.SSSSX||HH:mm:ss.SSSSSX||HH:mm:ss.SSSSSSX"
    }
  }
}
', true) ON CONFLICT (type_name) DO UPDATE SET definition = excluded.definition;
CREATE OR REPLACE FUNCTION zdb.get_highlight_analysis_info(index_name regclass, field text)
    RETURNS TABLE
            (
                type             text,
                normalizer       text,
                index_tokenizer  text,
                search_tokenizer text
            )
    LANGUAGE sql
AS
$$
WITH mapping AS (SELECT jsonb_extract_path(
                                zdb.index_mapping(index_name),
                                VARIADIC ARRAY [zdb.index_name(index_name), 'mappings', 'properties'] ||
                                         string_to_array(replace(field, '.', '.properties.'), '.')) AS mapping)
SELECT mapping ->> 'type'                                        AS type,
       mapping ->> 'normalizer'                                  AS normalizer,
       mapping ->> 'analyzer'        AS index_analyzer,
       mapping ->> 'search_analyzer' AS search_analyzer
FROM mapping;
$$;

