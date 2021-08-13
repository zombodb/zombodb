--
-- sql/_bootstrap.sql
--
DO LANGUAGE plpgsql $$
    DECLARE
        session_preload_libraries text;
    BEGIN
        session_preload_libraries = COALESCE (current_setting('session_preload_libraries'), '');
        IF (session_preload_libraries NOT LIKE '%zombodb.so%') THEN
            IF (session_preload_libraries = '') THEN
                session_preload_libraries = 'zombodb.so';
            ELSE
                session_preload_libraries = format('zombodb.so,%s', session_preload_libraries);
            END IF;

            EXECUTE format('ALTER DATABASE %I SET session_preload_libraries TO ''%s''', current_database(), session_preload_libraries);
        END IF;

    END;
$$;

-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.0.3 (6d3907ed9daabed6417a12fee2e5865bbadc0261)'
$$;

CREATE SCHEMA dsl;

GRANT ALL ON SCHEMA zdb TO PUBLIC;
GRANT ALL ON SCHEMA dsl TO PUBLIC;



--
-- sql/_mappings.sql
--
--
-- PG to ES type mapping support
--

--
-- filter/analyzer/mapping support
--

CREATE TABLE zdb.filters
(
    name       text                  NOT NULL PRIMARY KEY,
    definition jsonb                 NOT NULL,
    is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb.char_filters
(
    name       text                  NOT NULL PRIMARY KEY,
    definition jsonb                 NOT NULL,
    is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb.analyzers
(
    name       text                  NOT NULL PRIMARY KEY,
    definition jsonb                 NOT NULL,
    is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb.normalizers
(
    name       text                  NOT NULL PRIMARY KEY,
    definition jsonb                 NOT NULL,
    is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb.mappings
(
    table_name regclass NOT NULL,
    field_name text     NOT NULL,
    definition jsonb    NOT NULL,
    es_only    boolean  NOT NULL DEFAULT false,
    PRIMARY KEY (table_name, field_name)
);

CREATE TABLE zdb.type_mappings
(
    type_name  regtype               NOT NULL PRIMARY KEY,
    definition jsonb   DEFAULT NULL,
    is_default boolean DEFAULT false NOT NULL,
    funcid     regproc DEFAULT null
);

CREATE TABLE zdb.tokenizers
(
    name       text                  NOT NULL PRIMARY KEY,
    definition jsonb                 NOT NULL,
    is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb.type_conversions
(
    typeoid    regtype NOT NULL PRIMARY KEY,
    funcoid    regproc NOT NULL,
    is_default boolean DEFAULT false
);

CREATE TABLE zdb.similarities
(
    name       text NOT NULL PRIMARY KEY,
    definition jsonb
);

SELECT pg_catalog.pg_extension_config_dump('zdb.filters', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb.char_filters', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb.analyzers', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb.normalizers', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb.mappings', '');
SELECT pg_catalog.pg_extension_config_dump('zdb.tokenizers', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb.type_mappings', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb.type_conversions', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb.similarities', '');


CREATE OR REPLACE FUNCTION zdb.define_filter(name text, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.filters
WHERE name = $1;
INSERT INTO zdb.filters(name, definition)
VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb.define_char_filter(name text, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.char_filters
WHERE name = $1;
INSERT INTO zdb.char_filters(name, definition)
VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb.define_analyzer(name text, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.analyzers
WHERE name = $1;
INSERT INTO zdb.analyzers(name, definition)
VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb.define_normalizer(name text, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.normalizers
WHERE name = $1;
INSERT INTO zdb.normalizers(name, definition)
VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb.define_field_mapping(table_name regclass, field_name text, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.mappings
WHERE table_name = $1
  AND field_name = $2;
INSERT INTO zdb.mappings(table_name, field_name, definition)
VALUES ($1, $2, $3);
$$;

CREATE OR REPLACE FUNCTION zdb.define_es_only_field(table_name regclass, field_name text, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.mappings
WHERE table_name = $1
  AND field_name = $2;
INSERT INTO zdb.mappings(table_name, field_name, definition, es_only)
VALUES ($1, $2, $3, true);
$$;

CREATE OR REPLACE FUNCTION zdb.define_type_mapping(type_name regtype, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.type_mappings
WHERE type_name = $1;
INSERT INTO zdb.type_mappings(type_name, definition)
VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb.define_type_mapping(type_name regtype, funcid regproc) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.type_mappings
WHERE type_name = $1;
INSERT INTO zdb.type_mappings(type_name, funcid)
VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb.define_tokenizer(name text, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
DELETE
FROM zdb.tokenizers
WHERE name = $1;
INSERT INTO zdb.tokenizers(name, definition)
VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb.define_similarity(name text, definition json) RETURNS void
    LANGUAGE sql
    VOLATILE STRICT AS
$$
    DELETE FROM zdb.similarities WHERE name = $1;
    INSERT INTO zdb.similarities(name, definition) VALUES ($1, $2);
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


CREATE DOMAIN zdb.phrase AS text;
CREATE DOMAIN zdb.phrase_array AS text[];
CREATE DOMAIN zdb.fulltext AS text;
CREATE DOMAIN zdb.fulltext_with_shingles AS text;
CREATE DOMAIN zdb.zdb_standard AS text;

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

CREATE DOMAIN zdb.arabic AS text;
CREATE DOMAIN zdb.armenian AS text;
CREATE DOMAIN zdb.basque AS text;
CREATE DOMAIN zdb.brazilian AS text;
CREATE DOMAIN zdb.bulgarian AS text;
CREATE DOMAIN zdb.catalan AS text;
CREATE DOMAIN zdb.chinese AS text;
CREATE DOMAIN zdb.cjk AS text;
CREATE DOMAIN zdb.czech AS text;
CREATE DOMAIN zdb.danish AS text;
CREATE DOMAIN zdb.dutch AS text;
CREATE DOMAIN zdb.english AS text;
CREATE DOMAIN zdb.fingerprint AS text;
CREATE DOMAIN zdb.finnish AS text;
CREATE DOMAIN zdb.french AS text;
CREATE DOMAIN zdb.galician AS text;
CREATE DOMAIN zdb.german AS text;
CREATE DOMAIN zdb.greek AS text;
CREATE DOMAIN zdb.hindi AS text;
CREATE DOMAIN zdb.hungarian AS text;
CREATE DOMAIN zdb.indonesian AS text;
CREATE DOMAIN zdb.irish AS text;
CREATE DOMAIN zdb.italian AS text;
CREATE DOMAIN zdb.keyword AS character varying;
CREATE DOMAIN zdb.latvian AS text;
CREATE DOMAIN zdb.norwegian AS text;
CREATE DOMAIN zdb.persian AS text;
CREATE DOMAIN zdb.portuguese AS text;
CREATE DOMAIN zdb.romanian AS text;
CREATE DOMAIN zdb.russian AS text;
CREATE DOMAIN zdb.sorani AS text;
CREATE DOMAIN zdb.spanish AS text;
CREATE DOMAIN zdb.simple AS text;
CREATE DOMAIN zdb.standard AS text;
CREATE DOMAIN zdb.swedish AS text;
CREATE DOMAIN zdb.turkish AS text;
CREATE DOMAIN zdb.thai AS text;
CREATE DOMAIN zdb.whitespace AS text;

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

CREATE OR REPLACE FUNCTION zdb.define_type_conversion(typeoid regtype, funcoid regproc) RETURNS void
    VOLATILE STRICT
    LANGUAGE sql AS
$$
DELETE
FROM zdb.type_conversions
WHERE typeoid = $1;
INSERT INTO zdb.type_conversions(typeoid, funcoid)
VALUES ($1, $2);
$$;


--
-- permissions to do all the things to the tables defined here
---

GRANT ALL ON zdb.analyzers TO PUBLIC;
GRANT ALL ON zdb.char_filters TO PUBLIC;
GRANT ALL ON zdb.filters TO PUBLIC;
GRANT ALL ON zdb.mappings TO PUBLIC;
GRANT ALL ON zdb.similarities TO PUBLIC;
GRANT ALL ON zdb.tokenizers TO PUBLIC;
GRANT ALL ON zdb.type_mappings TO PUBLIC;
GRANT ALL ON zdb.normalizers TO PUBLIC;
GRANT ALL ON zdb.type_conversions TO PUBLIC;



--
-- sql/lib.generated.sql
--
-- ./src/lib.rs:42:0
CREATE OR REPLACE FUNCTION zdb."internal_version"() RETURNS text IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'internal_version_wrapper';
-- ./src/lib.rs:63:0
CREATE OR REPLACE FUNCTION zdb."ctid"("as_u64" bigint) RETURNS tid IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'ctid_wrapper';



--
-- sql/zdbquery_mod.generated.sql
--
CREATE TYPE pg_catalog.sortdirection AS ENUM (
'asc',
'desc'
);
CREATE TYPE pg_catalog.sortmode AS ENUM (
'min',
'max',
'sum',
'avg',
'median'
);
CREATE TYPE pg_catalog.zdbquery;
CREATE OR REPLACE FUNCTION pg_catalog.zdbquery_in(cstring) RETURNS pg_catalog.zdbquery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'zdbquery_in_wrapper';
CREATE OR REPLACE FUNCTION pg_catalog.zdbquery_out(pg_catalog.zdbquery) RETURNS cstring IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'zdbquery_out_wrapper';
CREATE TYPE pg_catalog.zdbquery (
                                INTERNALLENGTH = variable,
                                INPUT = pg_catalog.zdbquery_in,
                                OUTPUT = pg_catalog.zdbquery_out,
                                STORAGE = extended
                            );
CREATE TYPE pg_catalog.sortdescriptoroptions;
CREATE OR REPLACE FUNCTION pg_catalog.sortdescriptoroptions_in(cstring) RETURNS pg_catalog.sortdescriptoroptions IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'sortdescriptoroptions_in_wrapper';
CREATE OR REPLACE FUNCTION pg_catalog.sortdescriptoroptions_out(pg_catalog.sortdescriptoroptions) RETURNS cstring IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'sortdescriptoroptions_out_wrapper';
CREATE TYPE pg_catalog.sortdescriptoroptions (
                                INTERNALLENGTH = variable,
                                INPUT = pg_catalog.sortdescriptoroptions_in,
                                OUTPUT = pg_catalog.sortdescriptoroptions_out,
                                STORAGE = extended
                            );
CREATE TYPE pg_catalog.sortdescriptor;
CREATE OR REPLACE FUNCTION pg_catalog.sortdescriptor_in(cstring) RETURNS pg_catalog.sortdescriptor IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'sortdescriptor_in_wrapper';
CREATE OR REPLACE FUNCTION pg_catalog.sortdescriptor_out(pg_catalog.sortdescriptor) RETURNS cstring IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'sortdescriptor_out_wrapper';
CREATE TYPE pg_catalog.sortdescriptor (
                                INTERNALLENGTH = variable,
                                INPUT = pg_catalog.sortdescriptor_in,
                                OUTPUT = pg_catalog.sortdescriptor_out,
                                STORAGE = extended
                            );
-- ./src/zdbquery/mod.rs:878:0
CREATE OR REPLACE FUNCTION zdb."to_query_dsl"("query" ZDBQuery) RETURNS json IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'to_query_dsl_wrapper';
-- ./src/zdbquery/mod.rs:885:0
CREATE OR REPLACE FUNCTION zdb."to_queries_dsl"("queries" ZDBQuery[]) RETURNS json[] IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'to_queries_dsl_wrapper';



--
-- sql/zdbquery_cast.generated.sql
--
-- ./src/zdbquery/cast.rs:4:0
CREATE OR REPLACE FUNCTION zdb."zdbquery_from_text"("input" text) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_from_text_wrapper';
-- ./src/zdbquery/cast.rs:10:0
CREATE OR REPLACE FUNCTION zdb."zdbquery_from_json"("input" json) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_from_json_wrapper';
-- ./src/zdbquery/cast.rs:15:0
CREATE OR REPLACE FUNCTION zdb."zdbquery_from_jsonb"("input" JsonB) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_from_jsonb_wrapper';
-- ./src/zdbquery/cast.rs:20:0
CREATE OR REPLACE FUNCTION zdb."zdbquery_to_json"("input" ZDBQuery) RETURNS json IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_to_json_wrapper';
-- ./src/zdbquery/cast.rs:25:0
CREATE OR REPLACE FUNCTION zdb."zdbquery_to_jsonb"("input" ZDBQuery) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_to_jsonb_wrapper';
CREATE CAST (text AS zdbquery) WITH FUNCTION zdbquery_from_text(text) AS IMPLICIT;
CREATE CAST (json AS zdbquery) WITH FUNCTION zdbquery_from_json(json) AS IMPLICIT;
CREATE CAST (jsonb AS zdbquery) WITH FUNCTION zdbquery_from_jsonb(jsonb) AS IMPLICIT;
CREATE CAST (zdbquery AS json) WITH FUNCTION zdbquery_to_json(zdbquery) AS IMPLICIT;
CREATE CAST (zdbquery AS jsonb) WITH FUNCTION zdbquery_to_jsonb(zdbquery) AS IMPLICIT;



--
-- sql/query_dsl_geo.generated.sql
--
CREATE TYPE pg_catalog.geoshaperelation AS ENUM (
'INTERSECTS',
'DISJOINT',
'WITHIN',
'CONTAINS'
);
CREATE TYPE pg_catalog.geoboundingboxtype AS ENUM (
'indexed',
'memory'
);
-- ./src/query_dsl/geo.rs:24:0
CREATE OR REPLACE FUNCTION zdb."point_to_json"("point" point) RETURNS json IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'point_to_json_wrapper';
-- ./src/query_dsl/geo.rs:29:0
CREATE OR REPLACE FUNCTION zdb."point_array_to_json"("points" point[]) RETURNS json IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'point_array_to_json_wrapper';
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/geo.rs:43:4
CREATE OR REPLACE FUNCTION dsl."geo_shape"("field" text, "geojson_shape" json, "relation" GeoShapeRelation) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'geo_shape_wrapper';
-- ./src/query_dsl/geo.rs:57:4
CREATE OR REPLACE FUNCTION dsl."geo_bounding_box"("field" text, "bounding_box" box, "box_type" GeoBoundingBoxType DEFAULT 'memory') RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'geo_bounding_box_wrapper';
-- ./src/query_dsl/geo.rs:81:4
CREATE OR REPLACE FUNCTION dsl."geo_polygon"("field" text, "points" VARIADIC point[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'geo_polygon_wrapper';



--
-- sql/_postgis-support.sql
--
CREATE OR REPLACE FUNCTION zdb.enable_postgis_support(during_create_extension bool DEFAULT false) RETURNS boolean VOLATILE LANGUAGE plpgsql AS $func$
DECLARE
  postgis_installed boolean := (SELECT count(*) > 0 FROM pg_extension WHERE extname = 'postgis');
  geojson_namespace text := (SELECT (SELECT nspname FROM pg_namespace WHERE oid = pronamespace) FROM pg_proc WHERE proname = 'st_asgeojson' limit 1);
BEGIN

  IF postgis_installed THEN
    RAISE WARNING '[zombodb] Installing support for PostGIS';

    -- casting functions
    EXECUTE format('create or replace function zdb.geometry_to_json(%I.geometry, typmod integer DEFAULT -1) returns json parallel safe immutable strict language sql as $$
          SELECT CASE WHEN %I.postgis_typmod_type($2) = ''Point'' THEN
                    zdb.point_to_json(%I.st_transform($1, 4326)::point)::json
                 ELSE
                    %I.st_asgeojson(%I.st_transform($1, 4326))::json
                 END
          $$;',
      geojson_namespace, geojson_namespace, geojson_namespace, geojson_namespace, geojson_namespace);
    EXECUTE format('create or replace function zdb.geography_to_json(%I.geography, typmod integer DEFAULT -1) returns json parallel safe immutable strict language sql as $$
          select zdb.geometry_to_json($1::%I.geometry, $2);
          $$;',
      geojson_namespace, geojson_namespace);

    EXECUTE format('create or replace function zdb.postgis_type_mapping_func(datatype regtype, typmod integer) returns jsonb parallel safe immutable strict language sql as $$
          SELECT CASE WHEN %I.postgis_typmod_type($2) = ''Point'' THEN
                    ''{"type":"geo_point"}''::jsonb
                 ELSE
                    ''{"type":"geo_shape"}''::jsonb
                 END
          $$;', geojson_namespace);

    -- zdb type mappings
    EXECUTE format($$ SELECT zdb.define_type_mapping('%I.geometry'::regtype,  'zdb.postgis_type_mapping_func'::regproc); $$, geojson_namespace);
    EXECUTE format($$ SELECT zdb.define_type_mapping('%I.geography'::regtype, 'zdb.postgis_type_mapping_func'::regproc); $$, geojson_namespace);

    -- zdb type conversions
    EXECUTE format($$ SELECT zdb.define_type_conversion('%I.geometry'::regtype, 'zdb.geometry_to_json'::regproc); $$, geojson_namespace);
    EXECUTE format($$ SELECT zdb.define_type_conversion('%I.geography'::regtype, 'zdb.geography_to_json'::regproc); $$, geojson_namespace);

    IF during_create_extension = false THEN
      EXECUTE 'ALTER EXTENSION zombodb ADD FUNCTION zdb.geometry_to_json';
      EXECUTE 'ALTER EXTENSION zombodb ADD FUNCTION zdb.geography_to_json';
    END IF;

  END IF;

  RETURN postgis_installed;
END;
$func$;

DO LANGUAGE plpgsql $$
  DECLARE
    postgis_installed boolean := (SELECT count(*) > 0 FROM pg_extension WHERE extname = 'postgis');
  BEGIN
    IF postgis_installed THEN
      PERFORM zdb.enable_postgis_support(true);
    END IF;
  END;
$$;



--
-- sql/_type-conversions.sql
--
--
-- custom type conversions for some built-in postgres types
--

INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default)
VALUES ('point'::regtype, 'zdb.point_to_json'::regproc, true);
INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default)
VALUES ('point[]'::regtype, 'zdb.point_array_to_json'::regproc, true);

CREATE OR REPLACE FUNCTION zdb.bytea_to_json(bytea) RETURNS json
    PARALLEL SAFE IMMUTABLE STRICT
    LANGUAGE sql AS
$$
SELECT to_json(encode($1, 'base64'));
$$;

INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default)
VALUES ('bytea'::regtype, 'zdb.bytea_to_json'::regproc, true);



--
-- sql/access_method_mod.generated.sql
--
-- ./src/access_method/mod.rs:11:0
CREATE OR REPLACE FUNCTION amhandler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'amhandler_wrapper';
CREATE ACCESS METHOD zombodb TYPE INDEX HANDLER amhandler;



--
-- sql/zdbquery_opclass.generated.sql
--
-- ./src/zdbquery/opclass.rs:8:0
CREATE OR REPLACE FUNCTION zdb."anyelement_cmpfunc"("element" anyelement, "query" ZDBQuery) RETURNS bool IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'anyelement_cmpfunc_wrapper';
-- ./src/zdbquery/opclass.rs:98:0
CREATE OR REPLACE FUNCTION zdb."restrict"("root" internal, "_operator_oid" oid, "args" internal, "var_relid" integer) RETURNS double precision IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'restrict_wrapper';
CREATE OPERATOR pg_catalog.==> (
    PROCEDURE = anyelement_cmpfunc,
    RESTRICT = restrict,
    LEFTARG = anyelement,
    RIGHTARG = zdbquery
);

CREATE OPERATOR CLASS anyelement_zdb_ops DEFAULT FOR TYPE anyelement USING zombodb AS
    OPERATOR 1 pg_catalog.==>(anyelement, zdbquery),
--    OPERATOR 2 pg_catalog.==|(anyelement, zdbquery[]),
--    OPERATOR 3 pg_catalog.==&(anyelement, zdbquery[]),
--    OPERATOR 4 pg_catalog.==!(anyelement, zdbquery[]),
    STORAGE anyelement;



--
-- sql/misc_mod.generated.sql
--
-- ./src/misc/mod.rs:5:0
CREATE OR REPLACE FUNCTION zdb."query_tids"("index" regclass, "query" ZDBQuery) RETURNS tid[] IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'query_tids_wrapper';



--
-- sql/zql_ast.generated.sql
--
CREATE TYPE pg_catalog.proximitypart;
CREATE OR REPLACE FUNCTION pg_catalog.proximitypart_in(cstring) RETURNS pg_catalog.proximitypart IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'proximitypart_in_wrapper';
CREATE OR REPLACE FUNCTION pg_catalog.proximitypart_out(pg_catalog.proximitypart) RETURNS cstring IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'proximitypart_out_wrapper';
CREATE TYPE pg_catalog.proximitypart (
                                INTERNALLENGTH = variable,
                                INPUT = pg_catalog.proximitypart_in,
                                OUTPUT = pg_catalog.proximitypart_out,
                                STORAGE = extended
                            );



--
-- sql/query_dsl_term.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/term.rs:22:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" text, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_str_wrapper';
-- ./src/query_dsl/term.rs:35:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" bool, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_bool_wrapper';
-- ./src/query_dsl/term.rs:48:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" smallint, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_i16_wrapper';
-- ./src/query_dsl/term.rs:61:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" integer, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_i32_wrapper';
-- ./src/query_dsl/term.rs:74:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" bigint, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_i64_wrapper';
-- ./src/query_dsl/term.rs:87:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" real, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_f32_wrapper';
-- ./src/query_dsl/term.rs:100:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" double precision, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_f64_wrapper';
-- ./src/query_dsl/term.rs:113:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" time, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_time_wrapper';
-- ./src/query_dsl/term.rs:126:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" date, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_date_wrapper';
-- ./src/query_dsl/term.rs:139:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" time with time zone, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_time_with_timezone_wrapper';
-- ./src/query_dsl/term.rs:152:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" timestamp without time zone, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_timestamp_wrapper';
-- ./src/query_dsl/term.rs:165:4
CREATE OR REPLACE FUNCTION dsl."term"("field" text, "value" timestamp with time zone, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'term_timestamp_with_timezone_wrapper';



--
-- sql/query_dsl_terms.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/terms.rs:14:4
CREATE OR REPLACE FUNCTION dsl."terms"("field" text, "values" VARIADIC text[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_str_wrapper';
-- ./src/query_dsl/terms.rs:22:4
CREATE OR REPLACE FUNCTION dsl."terms"("field" text, "values" VARIADIC bool[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_bool_wrapper';
-- ./src/query_dsl/terms.rs:30:4
CREATE OR REPLACE FUNCTION dsl."terms"("field" text, "values" VARIADIC smallint[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_i16_wrapper';
-- ./src/query_dsl/terms.rs:38:4
CREATE OR REPLACE FUNCTION dsl."terms"("field" text, "values" VARIADIC integer[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_i32_wrapper';
-- ./src/query_dsl/terms.rs:46:4
CREATE OR REPLACE FUNCTION dsl."terms"("field" text, "values" VARIADIC bigint[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_i64_wrapper';
-- ./src/query_dsl/terms.rs:54:4
CREATE OR REPLACE FUNCTION dsl."terms"("field" text, "values" VARIADIC real[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_f32_wrapper';
-- ./src/query_dsl/terms.rs:62:4
CREATE OR REPLACE FUNCTION dsl."terms"("field" text, "values" VARIADIC double precision[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_f64_wrapper';



--
-- sql/query_dsl_prefix.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/prefix.rs:11:4
CREATE OR REPLACE FUNCTION dsl."prefix"("field" text, "value" text) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'prefix_wrapper';



--
-- sql/query_dsl_field_exists.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/field_exists.rs:11:4
CREATE OR REPLACE FUNCTION dsl."field_exists"("field" text) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'field_exists_wrapper';



--
-- sql/query_dsl_field_missing.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/field_missing.rs:11:4
CREATE OR REPLACE FUNCTION dsl."field_missing"("field" text) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'field_missing_wrapper';



--
-- sql/query_dsl_match_all.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/match_all.rs:11:4
CREATE OR REPLACE FUNCTION dsl."match_all"("boost" real DEFAULT '1.0') RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'match_all_wrapper';
-- ./src/query_dsl/match_all.rs:28:4
CREATE OR REPLACE FUNCTION dsl."match_none"() RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'match_none_wrapper';



--
-- sql/query_dsl_terms_array.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/terms_array.rs:9:4
CREATE OR REPLACE FUNCTION dsl."terms_array"("fieldname" text, "array" anyarray) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_array_wrapper';



--
-- sql/query_dsl_terms_lookup.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/terms_lookup.rs:22:4
CREATE OR REPLACE FUNCTION dsl."terms_lookup"("field" text, "index" text, "id" text, "path" text, "routing" text DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_lookup_wrapper';



--
-- sql/query_dsl_limit.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/limit.rs:5:4
CREATE OR REPLACE FUNCTION dsl."limit"("limit" bigint, "query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'limit_wrapper';
-- ./src/query_dsl/limit.rs:13:4
CREATE OR REPLACE FUNCTION dsl."offset"("offset" bigint, "query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'offset_wrapper';
-- ./src/query_dsl/limit.rs:21:4
CREATE OR REPLACE FUNCTION dsl."offset_limit"("offset" bigint, "limit" bigint, "query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'offset_limit_wrapper';
-- ./src/query_dsl/limit.rs:27:4
CREATE OR REPLACE FUNCTION dsl."min_score"("min_score" double precision, "query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'min_score_wrapper';
-- ./src/query_dsl/limit.rs:32:4
CREATE OR REPLACE FUNCTION dsl."row_estimate"("row_estimate" bigint, "query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'row_estimate_wrapper';



--
-- sql/query_dsl_sort.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/sort.rs:12:4
CREATE OR REPLACE FUNCTION dsl."sd"("field" text, "order" SortDirection, "mode" SortMode DEFAULT NULL) RETURNS SortDescriptor IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sd_wrapper';
-- ./src/query_dsl/sort.rs:29:4
CREATE OR REPLACE FUNCTION dsl."sd_nested"("field" text, "order" SortDirection, "nested_path" text, "nested_filter" ZDBQuery DEFAULT NULL, "mode" SortMode DEFAULT NULL) RETURNS SortDescriptor IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sd_nested_wrapper';
-- ./src/query_dsl/sort.rs:52:4
CREATE OR REPLACE FUNCTION dsl."sort"("sort_field" text, "sort_direction" SortDirection, "zdbquery" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sort_wrapper';
-- ./src/query_dsl/sort.rs:65:4
CREATE OR REPLACE FUNCTION dsl."sort_many"("zdbquery" ZDBQuery, "sort_descriptors" VARIADIC SortDescriptor[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sort_many_wrapper';
-- ./src/query_dsl/sort.rs:70:4
CREATE OR REPLACE FUNCTION dsl."sort_direct"("sort_json" json, "zdbquery" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sort_direct_wrapper';



--
-- sql/query_dsl_fuzzy.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/fuzzy.rs:22:4
CREATE OR REPLACE FUNCTION dsl."fuzzy"("field" text, "value" text, "boost" real DEFAULT NULL, "fuzziness" integer DEFAULT NULL, "prefix_length" bigint DEFAULT NULL, "max_expansions" bigint DEFAULT '50', "transpositions" bool DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'fuzzy_wrapper';



--
-- sql/query_dsl_span.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/span.rs:12:4
CREATE OR REPLACE FUNCTION dsl."span_containing"("little" ZDBQuery, "big" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_containing_wrapper';
-- ./src/query_dsl/span.rs:24:4
CREATE OR REPLACE FUNCTION dsl."span_first"("query" ZDBQuery, "end" bigint) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_first_wrapper';
-- ./src/query_dsl/span.rs:34:4
CREATE OR REPLACE FUNCTION dsl."span_masking"("field" text, "query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_masking_wrapper';
-- ./src/query_dsl/span.rs:45:4
CREATE OR REPLACE FUNCTION dsl."span_multi"("query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_multi_wrapper';
-- ./src/query_dsl/span.rs:54:4
CREATE OR REPLACE FUNCTION dsl."span_near"("in_order" bool, "slop" bigint, "clauses" VARIADIC ZDBQuery[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_near_wrapper';
-- ./src/query_dsl/span.rs:92:4
CREATE OR REPLACE FUNCTION dsl."span_not"("include" ZDBQuery, "exclude" ZDBQuery, "pre_integer" bigint DEFAULT NULL, "post_integer" bigint DEFAULT NULL, "dis_integer" bigint DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_not_wrapper';
-- ./src/query_dsl/span.rs:117:4
CREATE OR REPLACE FUNCTION dsl."span_or"("clauses" VARIADIC ZDBQuery[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_or_wrapper';
-- ./src/query_dsl/span.rs:143:4
CREATE OR REPLACE FUNCTION dsl."span_term"("field" text, "value" text, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_term_wrapper';
-- ./src/query_dsl/span.rs:160:4
CREATE OR REPLACE FUNCTION dsl."span_within"("little" ZDBQuery, "big" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'span_within_wrapper';



--
-- sql/query_dsl_misc.generated.sql
--
CREATE TYPE pg_catalog.regexflags AS ENUM (
'all',
'complement',
'interval',
'intersection',
'anystring'
);
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/misc.rs:47:4
CREATE OR REPLACE FUNCTION dsl."wildcard"("field" text, "value" text, "boost" real DEFAULT '1.0') RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'wildcard_wrapper';
-- ./src/query_dsl/misc.rs:61:4
CREATE OR REPLACE FUNCTION dsl."regexp"("field" text, "regexp" text, "boost" real DEFAULT NULL, "flags" RegexFlags[] DEFAULT NULL, "max_determinized_states" integer DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'regexp_wrapper';
-- ./src/query_dsl/misc.rs:84:4
CREATE OR REPLACE FUNCTION dsl."script"("source" text, "params" json DEFAULT NULL, "lang" text DEFAULT 'painless') RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'script_wrapper';



--
-- sql/query_dsl_range.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/range.rs:38:4
CREATE OR REPLACE FUNCTION dsl."range"("field" text, "lt" text DEFAULT NULL, "gt" text DEFAULT NULL, "lte" text DEFAULT NULL, "gte" text DEFAULT NULL, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'range_str_wrapper';
-- ./src/query_dsl/range.rs:66:4
CREATE OR REPLACE FUNCTION dsl."range"("field" text, "lt" bigint DEFAULT NULL, "gt" bigint DEFAULT NULL, "lte" bigint DEFAULT NULL, "gte" bigint DEFAULT NULL, "boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'range_numeric_wrapper';



--
-- sql/query_dsl_constant_score.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/constant_score.rs:5:4
CREATE OR REPLACE FUNCTION dsl."constant_score"("boost" real, "query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'constant_score_wrapper';
-- ./src/query_dsl/constant_score.rs:11:4
CREATE OR REPLACE FUNCTION dsl."boosting"("positive_query" ZDBQuery, "negative_query" ZDBQuery, "negative_boost" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'boosting_wrapper';
-- ./src/query_dsl/constant_score.rs:24:4
CREATE OR REPLACE FUNCTION dsl."dis_max"("queries" ZDBQuery[], "boost" real DEFAULT NULL, "tie_breaker" real DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'dis_max_wrapper';



--
-- sql/query_dsl_bool.generated.sql
--
CREATE TYPE pg_catalog.boolquerypart;
CREATE OR REPLACE FUNCTION pg_catalog.boolquerypart_in(cstring) RETURNS pg_catalog.boolquerypart IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'boolquerypart_in_wrapper';
CREATE OR REPLACE FUNCTION pg_catalog.boolquerypart_out(pg_catalog.boolquerypart) RETURNS cstring IMMUTABLE STRICT PARALLEL SAFE LANGUAGE C AS 'MODULE_PATHNAME', 'boolquerypart_out_wrapper';
CREATE TYPE pg_catalog.boolquerypart (
                                INTERNALLENGTH = variable,
                                INPUT = pg_catalog.boolquerypart_in,
                                OUTPUT = pg_catalog.boolquerypart_out,
                                STORAGE = extended
                            );
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/bool.rs:15:4
CREATE OR REPLACE FUNCTION dsl."bool"("parts" VARIADIC BoolQueryPart[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'bool_wrapper';
-- ./src/query_dsl/bool.rs:43:4
CREATE OR REPLACE FUNCTION dsl."should"("queries" VARIADIC ZDBQuery[]) RETURNS BoolQueryPart IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'should_wrapper';
-- ./src/query_dsl/bool.rs:62:4
CREATE OR REPLACE FUNCTION dsl."must"("queries" VARIADIC ZDBQuery[]) RETURNS BoolQueryPart IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'must_wrapper';
-- ./src/query_dsl/bool.rs:81:4
CREATE OR REPLACE FUNCTION dsl."must_not"("queries" VARIADIC ZDBQuery[]) RETURNS BoolQueryPart IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'must_not_wrapper';
-- ./src/query_dsl/bool.rs:100:4
CREATE OR REPLACE FUNCTION dsl."filter"("queries" VARIADIC ZDBQuery[]) RETURNS BoolQueryPart IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'filter_wrapper';
-- ./src/query_dsl/bool.rs:119:4
CREATE OR REPLACE FUNCTION dsl."binary_and"("a" ZDBQuery, "b" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'binary_and_wrapper';
-- ./src/query_dsl/bool.rs:133:4
CREATE OR REPLACE FUNCTION dsl."and"("queries" VARIADIC ZDBQuery[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'and_wrapper';
-- ./src/query_dsl/bool.rs:152:4
CREATE OR REPLACE FUNCTION dsl."or"("queries" VARIADIC ZDBQuery[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'or_wrapper';
-- ./src/query_dsl/bool.rs:171:4
CREATE OR REPLACE FUNCTION dsl."not"("queries" VARIADIC ZDBQuery[]) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'not_wrapper';
-- ./src/query_dsl/bool.rs:190:4
CREATE OR REPLACE FUNCTION dsl."noteq"("query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'noteq_wrapper';



--
-- sql/query_dsl_matches.generated.sql
--
CREATE TYPE pg_catalog.zerotermsquery AS ENUM (
'none',
'all'
);
CREATE TYPE pg_catalog.operator AS ENUM (
'and',
'or'
);
CREATE TYPE pg_catalog.matchtype AS ENUM (
'best_fields',
'most_fields',
'cross_fields',
'phrase',
'phrase_prefix'
);
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/matches.rs:130:4
CREATE OR REPLACE FUNCTION dsl."match"("field" text, "query" text, "boost" real DEFAULT NULL, "analyzer" text DEFAULT NULL, "minimum_should_match" integer DEFAULT NULL, "lenient" bool DEFAULT NULL, "fuzziness" integer DEFAULT NULL, "fuzzy_rewrite" text DEFAULT NULL, "fuzzy_transpositions" bool DEFAULT NULL, "prefix_length" integer DEFAULT NULL, "cutoff_frequency" real DEFAULT NULL, "auto_generate_synonyms_phrase_query" bool DEFAULT NULL, "zero_terms_query" ZeroTermsQuery DEFAULT NULL, "operator" Operator DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'match_wrapper_wrapper';
-- ./src/query_dsl/matches.rs:171:4
CREATE OR REPLACE FUNCTION dsl."multi_match"("fields" text[], "query" text, "boost" real DEFAULT NULL, "analyzer" text DEFAULT NULL, "minimum_should_match" integer DEFAULT NULL, "lenient" bool DEFAULT NULL, "fuzziness" integer DEFAULT NULL, "fuzzy_rewrite" text DEFAULT NULL, "fuzzy_transpositions" bool DEFAULT NULL, "prefix_length" integer DEFAULT NULL, "cutoff_frequency" real DEFAULT NULL, "auto_generate_synonyms_phrase_query" bool DEFAULT NULL, "zero_terms_query" ZeroTermsQuery DEFAULT NULL, "operator" Operator DEFAULT NULL, "match_type" MatchType DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'multi_match_wrapper';
-- ./src/query_dsl/matches.rs:213:4
CREATE OR REPLACE FUNCTION dsl."match_phrase"("field" text, "query" text, "boost" real DEFAULT NULL, "slop" integer DEFAULT NULL, "analyzer" text DEFAULT NULL, "zero_terms_query" ZeroTermsQuery DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'match_phrase_wrapper';
-- ./src/query_dsl/matches.rs:238:4
CREATE OR REPLACE FUNCTION dsl."phrase"("field" text, "query" text, "boost" real DEFAULT NULL, "slop" integer DEFAULT NULL, "analyzer" text DEFAULT NULL, "zero_terms_query" ZeroTermsQuery DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'phrase_wrapper';
-- ./src/query_dsl/matches.rs:250:4
CREATE OR REPLACE FUNCTION dsl."match_phrase_prefix"("field" text, "query" text, "boost" real DEFAULT NULL, "slop" integer DEFAULT NULL, "analyzer" text DEFAULT NULL, "maxexpansion" integer DEFAULT NULL, "zero_terms_query" ZeroTermsQuery DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'match_phrase_prefix_wrapper';



--
-- sql/query_dsl_nested.generated.sql
--
CREATE TYPE pg_catalog.scoremode AS ENUM (
'avg',
'sum',
'min',
'max',
'none'
);
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/nested.rs:21:4
CREATE OR REPLACE FUNCTION dsl."nested"("path" text, "query" ZDBQuery, "score_mode" ScoreMode DEFAULT 'avg', "ignore_unmapped" bool DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'nested_wrapper';



--
-- sql/query_dsl_datetime_range.generated.sql
--
CREATE TYPE pg_catalog.relation AS ENUM (
'intersects',
'contains',
'within'
);
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/datetime_range.rs:39:4
CREATE OR REPLACE FUNCTION dsl."datetime_range"("field" text, "lt" date DEFAULT NULL, "gt" date DEFAULT NULL, "lte" date DEFAULT NULL, "gte" date DEFAULT NULL, "boost" real DEFAULT NULL, "relation" Relation DEFAULT 'intersects') RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'datetime_range_date_wrapper';
-- ./src/query_dsl/datetime_range.rs:63:4
CREATE OR REPLACE FUNCTION dsl."datetime_range"("field" text, "lt" time DEFAULT NULL, "gt" time DEFAULT NULL, "lte" time DEFAULT NULL, "gte" time DEFAULT NULL, "boost" real DEFAULT NULL, "relation" Relation DEFAULT 'intersects') RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'datetime_range_time_wrapper';
-- ./src/query_dsl/datetime_range.rs:87:4
CREATE OR REPLACE FUNCTION dsl."datetime_range"("field" text, "lt" timestamp without time zone DEFAULT NULL, "gt" timestamp without time zone DEFAULT NULL, "lte" timestamp without time zone DEFAULT NULL, "gte" timestamp without time zone DEFAULT NULL, "boost" real DEFAULT NULL, "relation" Relation DEFAULT 'intersects') RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'datetime_range_time_stamp_wrapper';
-- ./src/query_dsl/datetime_range.rs:111:4
CREATE OR REPLACE FUNCTION dsl."datetime_range"("field" text, "lt" timestamp with time zone DEFAULT NULL, "gt" timestamp with time zone DEFAULT NULL, "lte" timestamp with time zone DEFAULT NULL, "gte" timestamp with time zone DEFAULT NULL, "boost" real DEFAULT NULL, "relation" Relation DEFAULT 'intersects') RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'datetime_range_timestamp_with_timezone_wrapper';
-- ./src/query_dsl/datetime_range.rs:135:4
CREATE OR REPLACE FUNCTION dsl."datetime_range"("field" text, "lt" time with time zone DEFAULT NULL, "gt" time with time zone DEFAULT NULL, "lte" time with time zone DEFAULT NULL, "gte" time with time zone DEFAULT NULL, "boost" real DEFAULT NULL, "relation" Relation DEFAULT 'intersects') RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'datetime_range_time_with_timezone_wrapper';



--
-- sql/query_dsl_query_string.generated.sql
--
CREATE TYPE pg_catalog.querystringdefaultoperator AS ENUM (
'and',
'or'
);
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/query_string.rs:64:4
CREATE OR REPLACE FUNCTION dsl."query_string"("query" text, "default_field" text DEFAULT NULL, "allow_leading_wildcard" bool DEFAULT NULL, "analyze_wildcard" bool DEFAULT NULL, "analyzer" text DEFAULT NULL, "auto_generate_synonyms_phrase_query" bool DEFAULT NULL, "boost" real DEFAULT NULL, "default_operator" QueryStringDefaultOperator DEFAULT NULL, "enable_position_increments" bool DEFAULT NULL, "fields" text[] DEFAULT NULL, "fuzziness" integer DEFAULT NULL, "fuzzy_max_expansions" bigint DEFAULT NULL, "fuzzy_transpositions" bool DEFAULT NULL, "fuzzy_prefix_length" bigint DEFAULT NULL, "lenient" bool DEFAULT NULL, "max_determinized_states" bigint DEFAULT NULL, "minimum_should_match" integer DEFAULT NULL, "quote_analyzer" text DEFAULT NULL, "phrase_slop" bigint DEFAULT NULL, "quote_field_suffix" text DEFAULT NULL, "time_zone" text DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'query_string_wrapper';



--
-- sql/query_dsl_more_like_this.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/more_like_this.rs:74:4
CREATE OR REPLACE FUNCTION dsl."more_like_this"("like" text[], "stop_words" text[] DEFAULT ARRAY['http', 'span', 'class', 'flashtext', 'let', 'its', 'may', 'well', 'got', 'too', 'them', 'really', 'new', 'set', 'please', 'how', 'our', 'from', 'sent', 'subject', 'sincerely', 'thank', 'thanks', 'just', 'get', 'going', 'were', 'much', 'can', 'also', 'she', 'her', 'him', 'his', 'has', 'been', 'ok', 'still', 'okay', 'does', 'did', 'about', 'yes', 'you', 'your', 'when', 'know', 'have', 'who', 'what', 'where', 'sir', 'page', 'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by', 'for', 'if', 'in', 'into', 'is', 'it', 'no', 'not', 'of', 'on', 'or', 'such', 'that', 'the', 'their', 'than', 'then', 'there', 'these', 'they', 'this', 'to', 'was', 'will', 'with'], "fields" text[] DEFAULT NULL, "boost" real DEFAULT NULL, "unlike" text DEFAULT NULL, "analyzer" text DEFAULT NULL, "minimum_should_match" integer DEFAULT NULL, "boost_terms" real DEFAULT NULL, "include" bool DEFAULT NULL, "min_term_freq" bigint DEFAULT NULL, "max_query_terms" bigint DEFAULT NULL, "min_doc_freq" bigint DEFAULT NULL, "max_doc_freq" bigint DEFAULT NULL, "min_word_length" bigint DEFAULT NULL, "max_word_length" bigint DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'more_like_this_with_array_wrapper';
-- ./src/query_dsl/more_like_this.rs:121:4
CREATE OR REPLACE FUNCTION dsl."more_like_this"("like" text, "fields" text[] DEFAULT null, "stop_words" text[] DEFAULT ARRAY['http', 'span', 'class', 'flashtext', 'let', 'its', 'may', 'well', 'got', 'too', 'them', 'really', 'new', 'set', 'please', 'how', 'our', 'from', 'sent', 'subject', 'sincerely', 'thank', 'thanks', 'just', 'get', 'going', 'were', 'much', 'can', 'also', 'she', 'her', 'him', 'his', 'has', 'been', 'ok', 'still', 'okay', 'does', 'did', 'about', 'yes', 'you', 'your', 'when', 'know', 'have', 'who', 'what', 'where', 'sir', 'page', 'a', 'an', 'and', 'are', 'as', 'at', 'be', 'but', 'by', 'for', 'if', 'in', 'into', 'is', 'it', 'no', 'not', 'of', 'on', 'or', 'such', 'that', 'the', 'their', 'than', 'then', 'there', 'these', 'they', 'this', 'to', 'was', 'will', 'with'], "boost" real DEFAULT NULL, "unlike" text DEFAULT NULL, "analyzer" text DEFAULT NULL, "minimum_should_match" integer DEFAULT NULL, "boost_terms" real DEFAULT NULL, "include" bool DEFAULT NULL, "min_term_freq" bigint DEFAULT NULL, "max_query_terms" bigint DEFAULT NULL, "min_doc_freq" bigint DEFAULT NULL, "max_doc_freq" bigint DEFAULT NULL, "min_word_length" bigint DEFAULT NULL, "max_word_length" bigint DEFAULT NULL) RETURNS ZDBQuery IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'more_like_this_without_array_wrapper';



--
-- sql/elasticsearch_mod.generated.sql
--
CREATE TYPE pg_catalog.arbitraryrequesttype AS ENUM (
'GET',
'POST',
'PUT',
'DELETE'
);
-- ./src/elasticsearch/mod.rs:528:0
CREATE OR REPLACE FUNCTION zdb."request"("index" regclass, "endpoint" text, "method" ArbitraryRequestType DEFAULT 'GET', "post_data" JsonB DEFAULT NULL, "null_on_error" bool DEFAULT 'false') RETURNS text VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'request_wrapper';



--
-- sql/elasticsearch_profile_query.generated.sql
--
-- ./src/elasticsearch/profile_query.rs:40:0
CREATE OR REPLACE FUNCTION zdb."profile_query"("index" regclass, "query" ZDBQuery) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'profile_query_wrapper';



--
-- sql/elasticsearch_aggregates_terms.generated.sql
--
CREATE TYPE pg_catalog.termsorderby AS ENUM (
'count',
'term',
'key',
'reverse_count',
'reverse_term',
'reverse_key'
);
-- ./src/elasticsearch/aggregates/terms.rs:28:0
CREATE OR REPLACE FUNCTION zdb."terms"("index" regclass, "field_name" text, "query" ZDBQuery, "size_limit" integer DEFAULT '2147483647', "order_by" TermsOrderBy DEFAULT 'count') RETURNS TABLE ("term" text, "doc_count" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_wrapper';
-- ./src/elasticsearch/aggregates/terms.rs:63:0
CREATE OR REPLACE FUNCTION zdb."tally"("index" regclass, "field_name" text, "stem" text, "query" ZDBQuery, "size_limit" integer DEFAULT '2147483647', "order_by" TermsOrderBy DEFAULT 'count', "shard_size" integer DEFAULT '2147483647', "count_nulls" bool DEFAULT 'true') RETURNS TABLE ("term" text, "count" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'tally_not_nested_wrapper';
-- ./src/elasticsearch/aggregates/terms.rs:87:0
CREATE OR REPLACE FUNCTION zdb."tally"("index" regclass, "field_name" text, "is_nested" bool, "stem" text, "query" ZDBQuery, "size_limit" integer DEFAULT '2147483647', "order_by" TermsOrderBy DEFAULT 'count', "shard_size" integer DEFAULT '2147483647', "count_nulls" bool DEFAULT 'true') RETURNS TABLE ("term" text, "count" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'tally_wrapper';
-- ./src/elasticsearch/aggregates/terms.rs:342:0
CREATE OR REPLACE FUNCTION zdb."terms_array"(
"index" regclass,
"field_name" text,
"query" ZDBQuery,
"size_limit" integer DEFAULT '2147483647',
"order_by" TermsOrderBy DEFAULT 'count')
RETURNS text[]
IMMUTABLE PARALLEL SAFE
LANGUAGE c AS 'MODULE_PATHNAME', 'terms_array_agg_wrapper';



--
-- sql/elasticsearch_aggregates_significant_terms.generated.sql
--
-- ./src/elasticsearch/aggregates/significant_terms.rs:8:0
CREATE OR REPLACE FUNCTION zdb."significant_terms"("index" regclass, "field" text, "query" ZDBQuery, "include" text DEFAULT '.*', "size_limit" integer DEFAULT '2147483647', "min_doc_count" integer DEFAULT '3') RETURNS TABLE ("term" text, "doc_count" bigint, "score" real, "bg_count" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'significant_terms_wrapper';



--
-- sql/elasticsearch_aggregates_significant_text.generated.sql
--
-- ./src/elasticsearch/aggregates/significant_text.rs:8:0
CREATE OR REPLACE FUNCTION zdb."significant_text"("index" regclass, "field" text, "query" ZDBQuery, "size" integer DEFAULT '10', "filter_duplicate_text" bool DEFAULT 'true') RETURNS TABLE ("term" text, "doc_count" bigint, "score" double precision, "bg_count" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'significant_text_wrapper';



--
-- sql/access_method_triggers.generated.sql
--
-- ./src/access_method/triggers.rs:6:0
CREATE OR REPLACE FUNCTION zdb.zdb_update_trigger() RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_update_trigger_wrapper';
-- ./src/access_method/triggers.rs:56:0
CREATE OR REPLACE FUNCTION zdb.zdb_delete_trigger() RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_delete_trigger_wrapper';



--
-- sql/elasticsearch_aggregates_histogram.generated.sql
--
-- ./src/elasticsearch/aggregates/histogram.rs:7:0
CREATE OR REPLACE FUNCTION zdb."histogram"("index" regclass, "field" text, "query" ZDBQuery, "interval" double precision, "min_doc_count" integer DEFAULT '0') RETURNS TABLE ("term" numeric, "doc_count" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'histogram_wrapper';



--
-- sql/elasticsearch_aggregates_date_histogram.generated.sql
--
CREATE TYPE pg_catalog.calendarinterval AS ENUM (
'minute',
'hour',
'day',
'week',
'month',
'quarter',
'year'
);
-- ./src/elasticsearch/aggregates/date_histogram.rs:25:0
CREATE OR REPLACE FUNCTION zdb."date_histogram"("index" regclass, "field" text, "query" ZDBQuery, "calendar_interval" CalendarInterval DEFAULT NULL, "fixed_interval" text DEFAULT NULL, "time_zone" text DEFAULT '+00:00', "format" text DEFAULT 'yyyy-MM-dd') RETURNS TABLE ("key_as_string" text, "term" bigint, "doc_count" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'date_histogram_wrapper';



--
-- sql/elasticsearch_aggregates_adjacency_matrix.generated.sql
--
-- ./src/elasticsearch/aggregates/adjacency_matrix.rs:10:0
CREATE OR REPLACE FUNCTION zdb."adjacency_matrix"("index" regclass, "labels" text[], "filters" ZDBQuery[]) RETURNS TABLE ("term" text, "doc_count" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'adjacency_matrix_wrapper';
  

CREATE OR REPLACE FUNCTION zdb.adjacency_matrix_2x2(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT term, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE term = labels[1]),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term = labels[2])

$$;

CREATE OR REPLACE FUNCTION zdb.adjacency_matrix_3x3(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT term, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE term = labels[1]),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term = labels[2]),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term = labels[3])

$$;

CREATE OR REPLACE FUNCTION zdb.adjacency_matrix_4x4(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text, "4" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT term, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3], labels[4]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE term = labels[1]),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[4], labels[4]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term = labels[2]),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[4], labels[4]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term = labels[3]),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[4], labels[4]||'&'||labels[3]))
   UNION ALL
SELECT labels[4],
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[1], labels[1]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[2], labels[2]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[3], labels[3]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term = labels[4])

$$;

CREATE OR REPLACE FUNCTION zdb.adjacency_matrix_5x5(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text, "4" text, "5" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT term, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3], labels[4], labels[5]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE term = labels[1]),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[4], labels[4]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE term in (labels[1]||'&'||labels[5], labels[5]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term = labels[2]),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[4], labels[4]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE term in (labels[2]||'&'||labels[5], labels[5]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term = labels[3]),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[4], labels[4]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE term in (labels[3]||'&'||labels[5], labels[5]||'&'||labels[3]))
   UNION ALL
SELECT labels[4],
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[1], labels[1]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[2], labels[2]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[3], labels[3]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE term = labels[4]),
    (SELECT doc_count FROM matrix WHERE term in (labels[4]||'&'||labels[5], labels[5]||'&'||labels[4]))
   UNION ALL
SELECT labels[5],
    (SELECT doc_count FROM matrix WHERE term in (labels[5]||'&'||labels[1], labels[1]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE term in (labels[5]||'&'||labels[2], labels[2]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE term in (labels[5]||'&'||labels[3], labels[3]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE term in (labels[5]||'&'||labels[4], labels[4]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE term = labels[5])

$$;



--
-- sql/elasticsearch_aggregates_arbitrary_agg.generated.sql
--
-- ./src/elasticsearch/aggregates/arbitrary_agg.rs:5:0
CREATE OR REPLACE FUNCTION zdb."arbitrary_agg"("index" regclass, "query" ZDBQuery, "json" JsonB) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'arbitrary_agg_wrapper';



--
-- sql/access_method_options.generated.sql
--
-- ./src/access_method/options.rs:434:0
/*
we don't want any SQL generated for the "shadow" function, but we do want its '_wrapper' symbol
exported so that shadow indexes can reference it using whatever argument type they want
*/
-- ./src/access_method/options.rs:445:0
CREATE OR REPLACE FUNCTION zdb."determine_index"("relation" regclass) RETURNS regclass STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'determine_index_wrapper';
-- ./src/access_method/options.rs:455:0
CREATE OR REPLACE FUNCTION zdb."index_links"("relation" regclass) RETURNS text[] STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_links_wrapper';
-- ./src/access_method/options.rs:461:0
CREATE OR REPLACE FUNCTION zdb."index_name"("index_relation" regclass) RETURNS text STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_name_wrapper';
-- ./src/access_method/options.rs:468:0
CREATE OR REPLACE FUNCTION zdb."index_alias"("index_relation" regclass) RETURNS text STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_alias_wrapper';
-- ./src/access_method/options.rs:475:0
CREATE OR REPLACE FUNCTION zdb."index_url"("index_relation" regclass) RETURNS text STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_url_wrapper';
-- ./src/access_method/options.rs:482:0
CREATE OR REPLACE FUNCTION zdb."index_type_name"("index_relation" regclass) RETURNS text STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_type_name_wrapper';
-- ./src/access_method/options.rs:489:0
CREATE OR REPLACE FUNCTION zdb."index_mapping"("index_relation" regclass) RETURNS JsonB STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_mapping_wrapper';
-- ./src/access_method/options.rs:499:0
CREATE OR REPLACE FUNCTION zdb."index_settings"("index_relation" regclass) RETURNS JsonB STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_settings_wrapper';
-- ./src/access_method/options.rs:509:0
CREATE OR REPLACE FUNCTION zdb."index_options"("index_relation" regclass) RETURNS text[] STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_options_wrapper';
-- ./src/access_method/options.rs:516:0
CREATE OR REPLACE FUNCTION zdb."index_field_lists"("index_relation" regclass) RETURNS TABLE ("fieldname" text, "fields" text[]) STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_field_lists_wrapper';
-- ./src/access_method/options.rs:526:0
CREATE OR REPLACE FUNCTION zdb."field_mapping"("index_relation" regclass, "field" text) RETURNS JsonB STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'field_mapping_wrapper';



--
-- sql/elasticsearch_aggregates_count.generated.sql
--
-- ./src/elasticsearch/aggregates/count.rs:6:0
CREATE OR REPLACE FUNCTION zdb."count"("index" regclass, "query" ZDBQuery) RETURNS bigint IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'count_wrapper';
-- ./src/elasticsearch/aggregates/count.rs:17:0
CREATE OR REPLACE FUNCTION zdb."raw_count"("index" regclass, "query" ZDBQuery) RETURNS bigint IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'raw_count_wrapper';



--
-- sql/elasticsearch_aggregates_percentiles.generated.sql
--
-- ./src/elasticsearch/aggregates/percentiles.rs:7:0
CREATE OR REPLACE FUNCTION zdb."percentiles"("index" regclass, "field" text, "query" ZDBQuery, "percents" json DEFAULT NULL) RETURNS TABLE ("key" double precision, "value" numeric) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_wrapper';



--
-- sql/elasticsearch_aggregates_percentile_ranks.generated.sql
--
-- ./src/elasticsearch/aggregates/percentile_ranks.rs:7:0
CREATE OR REPLACE FUNCTION zdb."percentile_ranks"("index" regclass, "field" text, "query" ZDBQuery, "values" json) RETURNS TABLE ("key" double precision, "value" numeric) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentile_ranks_wrapper';



--
-- sql/access_method_options.generated.sql
--
-- ./src/access_method/options.rs:434:0
/*
we don't want any SQL generated for the "shadow" function, but we do want its '_wrapper' symbol
exported so that shadow indexes can reference it using whatever argument type they want
*/
-- ./src/access_method/options.rs:445:0
CREATE OR REPLACE FUNCTION zdb."determine_index"("relation" regclass) RETURNS regclass STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'determine_index_wrapper';
-- ./src/access_method/options.rs:455:0
CREATE OR REPLACE FUNCTION zdb."index_links"("relation" regclass) RETURNS text[] STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_links_wrapper';
-- ./src/access_method/options.rs:461:0
CREATE OR REPLACE FUNCTION zdb."index_name"("index_relation" regclass) RETURNS text STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_name_wrapper';
-- ./src/access_method/options.rs:468:0
CREATE OR REPLACE FUNCTION zdb."index_alias"("index_relation" regclass) RETURNS text STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_alias_wrapper';
-- ./src/access_method/options.rs:475:0
CREATE OR REPLACE FUNCTION zdb."index_url"("index_relation" regclass) RETURNS text STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_url_wrapper';
-- ./src/access_method/options.rs:482:0
CREATE OR REPLACE FUNCTION zdb."index_type_name"("index_relation" regclass) RETURNS text STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_type_name_wrapper';
-- ./src/access_method/options.rs:489:0
CREATE OR REPLACE FUNCTION zdb."index_mapping"("index_relation" regclass) RETURNS JsonB STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_mapping_wrapper';
-- ./src/access_method/options.rs:499:0
CREATE OR REPLACE FUNCTION zdb."index_settings"("index_relation" regclass) RETURNS JsonB STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_settings_wrapper';
-- ./src/access_method/options.rs:509:0
CREATE OR REPLACE FUNCTION zdb."index_options"("index_relation" regclass) RETURNS text[] STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_options_wrapper';
-- ./src/access_method/options.rs:516:0
CREATE OR REPLACE FUNCTION zdb."index_field_lists"("index_relation" regclass) RETURNS TABLE ("fieldname" text, "fields" text[]) STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'index_field_lists_wrapper';
-- ./src/access_method/options.rs:526:0
CREATE OR REPLACE FUNCTION zdb."field_mapping"("index_relation" regclass, "field" text) RETURNS JsonB STRICT VOLATILE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'field_mapping_wrapper';



--
-- sql/elasticsearch_aggregates_terms_two_level.generated.sql
--
CREATE TYPE pg_catalog.twoleveltermsorderby AS ENUM (
'count',
'term',
'key',
'reverse_count',
'reverse_term',
'reverse_key'
);
-- ./src/elasticsearch/aggregates/terms_two_level.rs:25:0
CREATE OR REPLACE FUNCTION zdb."terms_two_level"("index" regclass, "field_first" text, "field_second" text, "query" ZDBQuery, "order_by" TwoLevelTermsOrderBy DEFAULT 'count', "size_limit" integer DEFAULT '2147483647') RETURNS TABLE ("term_one" text, "term_two" text, "doc_count" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_two_level_wrapper';



--
-- sql/elasticsearch_aggregates_significant_terms_two_level.generated.sql
--
-- ./src/elasticsearch/aggregates/significant_terms_two_level.rs:8:0
CREATE OR REPLACE FUNCTION zdb."significant_terms_two_level"("index" regclass, "field_first" text, "field_second" text, "query" ZDBQuery, "size_limit" integer DEFAULT '2147483647') RETURNS TABLE ("term_one" text, "term_two" text, "doc_count" bigint, "score" double precision, "bg_count" bigint, "doc_count_error_upper_bound" bigint, "sum_other_doc_count" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'significant_terms_two_level_wrapper';



--
-- sql/elasticsearch_aggregates_filters.generated.sql
--
-- ./src/elasticsearch/aggregates/filters.rs:9:0
CREATE OR REPLACE FUNCTION zdb."filters"("index" regclass, "labels" text[], "filters" ZDBQuery[]) RETURNS TABLE ("term" text, "doc_count" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'filters_wrapper';



--
-- sql/elasticsearch_aggregates_matrix_stats.generated.sql
--
-- ./src/elasticsearch/aggregates/matrix_stats.rs:7:0
CREATE OR REPLACE FUNCTION zdb."matrix_stats"("index" regclass, "fields" text[], "query" ZDBQuery) RETURNS TABLE ("term" text, "count" bigint, "mean" numeric, "variance" numeric, "skewness" numeric, "kurtosis" numeric, "covariance" json, "correlation" json) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'matrix_stats_wrapper';



--
-- sql/zdbquery_mvcc.generated.sql
--
-- ./src/zdbquery/mvcc.rs:8:0
CREATE OR REPLACE FUNCTION zdb."internal_visibility_clause"("index_relation" regclass) RETURNS json STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'internal_visibility_clause_wrapper';
-- ./src/zdbquery/mvcc.rs:17:0
CREATE OR REPLACE FUNCTION zdb."wrap_with_visibility_clause"("index_relation" regclass, "query" ZDBQuery) RETURNS ZDBQuery STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'wrap_with_visibility_clause_wrapper';



--
-- sql/elasticsearch_aggregates_metrics.generated.sql
--
-- ./src/elasticsearch/aggregates/metrics.rs:7:0
CREATE OR REPLACE FUNCTION zdb."sum"("index" regclass, "field" text, "query" ZDBQuery) RETURNS numeric IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sum_wrapper';
-- ./src/elasticsearch/aggregates/metrics.rs:36:0
CREATE OR REPLACE FUNCTION zdb."avg"("index" regclass, "field" text, "query" ZDBQuery) RETURNS numeric IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'avg_wrapper';
-- ./src/elasticsearch/aggregates/metrics.rs:65:0
CREATE OR REPLACE FUNCTION zdb."cardinality"("index" regclass, "field" text, "query" ZDBQuery) RETURNS numeric IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'cardinality_wrapper';
-- ./src/elasticsearch/aggregates/metrics.rs:94:0
CREATE OR REPLACE FUNCTION zdb."max"("index" regclass, "field" text, "query" ZDBQuery) RETURNS numeric IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'max_wrapper';
-- ./src/elasticsearch/aggregates/metrics.rs:123:0
CREATE OR REPLACE FUNCTION zdb."min"("index" regclass, "field" text, "query" ZDBQuery) RETURNS numeric IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'min_wrapper';
-- ./src/elasticsearch/aggregates/metrics.rs:152:0
CREATE OR REPLACE FUNCTION zdb."missing"("index" regclass, "field" text, "query" ZDBQuery) RETURNS numeric IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'missing_wrapper';
-- ./src/elasticsearch/aggregates/metrics.rs:181:0
CREATE OR REPLACE FUNCTION zdb."value_count"("index" regclass, "field" text, "query" ZDBQuery) RETURNS numeric IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'value_count_wrapper';



--
-- sql/elasticsearch_aggregates_stats.generated.sql
--
-- ./src/elasticsearch/aggregates/stats.rs:7:0
CREATE OR REPLACE FUNCTION zdb."stats"("index" regclass, "field" text, "query" ZDBQuery) RETURNS TABLE ("count" bigint, "min" numeric, "max" numeric, "avg" numeric, "sum" numeric) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'stats_wrapper';



--
-- sql/elasticsearch_aggregates_range.generated.sql
--
-- ./src/elasticsearch/aggregates/range.rs:8:0
CREATE OR REPLACE FUNCTION zdb."range"("index" regclass, "field" text, "query" ZDBQuery, "range_array" json) RETURNS TABLE ("key" text, "from" numeric, "to" numeric, "doc_count" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'range_wrapper';



--
-- sql/elasticsearch_aggregates_date_range.generated.sql
--
-- ./src/elasticsearch/aggregates/date_range.rs:8:0
CREATE OR REPLACE FUNCTION zdb."date_range"("index" regclass, "field" text, "query" ZDBQuery, "date_range_array" json) RETURNS TABLE ("key" text, "from" numeric, "from_as_string" text, "to" numeric, "to_as_string" text, "doc_count" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'date_range_wrapper';



--
-- sql/elasticsearch_aggregates_extended_stats.generated.sql
--
-- ./src/elasticsearch/aggregates/extended_stats.rs:7:0
CREATE OR REPLACE FUNCTION zdb."extended_stats"("index" regclass, "field" text, "query" ZDBQuery, "sigma" bigint DEFAULT '0') RETURNS TABLE ("count" bigint, "min" numeric, "max" numeric, "avg" numeric, "sum" numeric, "sum_of_squares" numeric, "variance" numeric, "std_deviation" numeric, "upper" numeric, "lower" numeric) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'extended_stats_wrapper';



--
-- sql/elasticsearch_aggregates_top_hits.generated.sql
--
-- ./src/elasticsearch/aggregates/top_hits.rs:7:0
CREATE OR REPLACE FUNCTION zdb."top_hits"("index" regclass, "fields" text[], "query" ZDBQuery, "size_limit" bigint) RETURNS TABLE ("id" tid, "score" double precision, "source" json) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'top_hits_wrapper';



--
-- sql/elasticsearch_aggregates_top_hits_with_id.generated.sql
--
-- ./src/elasticsearch/aggregates/top_hits_with_id.rs:7:0
CREATE OR REPLACE FUNCTION zdb."top_hits_with_id"("index" regclass, "fields" text[], "query" ZDBQuery, "size_limit" bigint) RETURNS TABLE ("id" text, "score" double precision, "source" json) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'top_hits_with_id_wrapper';



--
-- sql/elasticsearch_aggregates_ip_range.generated.sql
--
-- ./src/elasticsearch/aggregates/ip_range.rs:8:0
CREATE OR REPLACE FUNCTION zdb."ip_range"("index" regclass, "field" text, "query" ZDBQuery, "range_array" json) RETURNS TABLE ("key" text, "from" inet, "to" inet, "doc_count" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'ip_range_wrapper';



--
-- sql/elasticsearch_aggregates_query.generated.sql
--
-- ./src/elasticsearch/aggregates/query.rs:5:0
CREATE OR REPLACE FUNCTION zdb."query"("index" regclass, "query" ZDBQuery) RETURNS SETOF tid IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'query_wrapper';
-- ./src/elasticsearch/aggregates/query.rs:22:0
CREATE OR REPLACE FUNCTION query_raw(index regclass, query zdbquery)
RETURNS SETOF tid SET zdb.ignore_visibility = true
IMMUTABLE STRICT ROWS 2500 LANGUAGE c AS 'MODULE_PATHNAME', 'query_raw_wrapper';



--
-- sql/elasticsearch_analyze.generated.sql
--
-- ./src/elasticsearch/analyze.rs:118:0
CREATE OR REPLACE FUNCTION zdb."analyze_text"("index" regclass, "analyzer" text, "text" text) RETURNS TABLE ("type" text, "token" text, "position" integer, "start_offset" bigint, "end_offset" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'analyze_text_wrapper';
-- ./src/elasticsearch/analyze.rs:137:0
CREATE OR REPLACE FUNCTION zdb."analyze_with_field"("index" regclass, "field" text, "text" text) RETURNS TABLE ("type" text, "token" text, "position" integer, "start_offset" bigint, "end_offset" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'analyze_with_field_wrapper';
-- ./src/elasticsearch/analyze.rs:156:0
CREATE OR REPLACE FUNCTION zdb."analyze_custom"("index" regclass, "field" text DEFAULT NULL, "text" text DEFAULT NULL, "tokenizer" text DEFAULT NULL, "normalizer" text DEFAULT NULL, "filter" text[] DEFAULT NULL, "char_filter" text[] DEFAULT NULL) RETURNS TABLE ("type" text, "token" text, "position" integer, "start_offset" bigint, "end_offset" bigint) IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'analyze_custom_wrapper';



--
-- sql/scoring_mod.generated.sql
--
-- ./src/scoring/mod.rs:5:0
CREATE OR REPLACE FUNCTION zdb."score"("ctid" tid) RETURNS double precision IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'score_wrapper';
-- ./src/scoring/mod.rs:30:0
CREATE OR REPLACE FUNCTION zdb."want_scores"("query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'want_scores_wrapper';



--
-- sql/highlighting_document_highlighter.generated.sql
--
-- ./src/highlighting/document_highlighter.rs:509:0
CREATE OR REPLACE FUNCTION zdb."highlight_term"("index" regclass, "field_name" text, "text" text, "token_to_highlight" text) RETURNS TABLE ("field_name" text, "term" text, "type" text, "position" integer, "start_offset" bigint, "end_offset" bigint) STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_term_wrapper';
-- ./src/highlighting/document_highlighter.rs:547:0
CREATE OR REPLACE FUNCTION zdb."highlight_phrase"("index" regclass, "field_name" text, "text" text, "tokens_to_highlight" text) RETURNS TABLE ("field_name" text, "term" text, "type" text, "position" integer, "start_offset" bigint, "end_offset" bigint) STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_phrase_wrapper';
-- ./src/highlighting/document_highlighter.rs:585:0
CREATE OR REPLACE FUNCTION zdb."highlight_wildcard"("index" regclass, "field_name" text, "text" text, "token_to_highlight" text) RETURNS TABLE ("field_name" text, "term" text, "type" text, "position" integer, "start_offset" bigint, "end_offset" bigint) STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_wildcard_wrapper';
-- ./src/highlighting/document_highlighter.rs:623:0
CREATE OR REPLACE FUNCTION zdb."highlight_regex"("index" regclass, "field_name" text, "text" text, "token_to_highlight" text) RETURNS TABLE ("field_name" text, "term" text, "type" text, "position" integer, "start_offset" bigint, "end_offset" bigint) STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_regex_wrapper';
-- ./src/highlighting/document_highlighter.rs:661:0
CREATE OR REPLACE FUNCTION zdb."highlight_fuzzy"("index" regclass, "field_name" text, "text" text, "token_to_highlight" text, "prefix" integer) RETURNS TABLE ("field_name" text, "term" text, "type" text, "position" integer, "start_offset" bigint, "end_offset" bigint) STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_fuzzy_wrapper';
-- ./src/highlighting/document_highlighter.rs:704:0
CREATE OR REPLACE FUNCTION zdb."highlight_proximity"("index" regclass, "field_name" text, "text" text, "prox_clause" ProximityPart[]) RETURNS TABLE ("field_name" text, "term" text, "type" text, "position" integer, "start_offset" bigint, "end_offset" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_proximity_wrapper';



--
-- sql/cat_mod.generated.sql
--
-- ./src/cat/mod.rs:5:0
CREATE OR REPLACE FUNCTION zdb."cat_request"("index" regclass, "endpoint" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'cat_request_wrapper';



--
-- sql/_cat-api.sql
--
--
-- _cat/ API support
--
CREATE OR REPLACE FUNCTION zdb.all_es_index_names() RETURNS SETOF text PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
SELECT zdb.index_name(oid::regclass) FROM pg_class WHERE relam = (SELECT oid FROM pg_am WHERE amname = 'zombodb');
$$;

CREATE OR REPLACE FUNCTION zdb._all_indices_cat_request(endpoint TEXT) RETURNS TABLE(url TEXT, response JSONB) PARALLEL SAFE IMMUTABLE STRICT LANGUAGE SQL AS $$
WITH clusters AS (SELECT
                      zdb.index_url(idx) url,
                      idx
                  FROM (SELECT DISTINCT ON (zdb.index_url(oid::regclass)) oid::regclass idx
                        FROM pg_class
                        WHERE relam = (SELECT oid
                                       FROM pg_am
                                       WHERE amname = 'zombodb')) x)
SELECT
    url,
    jsonb_array_elements(zdb.cat_request(idx, $1))
FROM clusters
$$;

CREATE OR REPLACE VIEW zdb.cat_aliases AS
SELECT
    url,
    response->>'alias' AS "alias",
    response->>'index' AS "index",
    response->>'filter' AS "filter",
    response->>'routing.index' AS "routing.index",
    response->>'routing.search' AS "routing.search"
FROM zdb._all_indices_cat_request('aliases')
WHERE response->>'index' IN (SELECT index from zdb.all_es_index_names() index);

CREATE OR REPLACE VIEW zdb.cat_allocation AS
SELECT
    url,
    (response->>'shards')::int AS "shards",
    (response->>'disk.indices')::bigint AS "disk.indices",
    (response->>'disk.used')::bigint AS "disk.used",
    (response->>'disk.avail')::bigint AS "disk.avail",
    (response->>'disk.total')::bigint AS "disk.total",
    (response->>'disk.percent')::real AS "disk.percent",
    (response->>'host') AS "host",
    (response->>'ip')::inet AS "ip",
    (response->>'node') AS "node"
FROM zdb._all_indices_cat_request('allocation');

CREATE OR REPLACE VIEW zdb.cat_count AS
SELECT
    url,
    (response->>'epoch')::bigint AS "epoch",
    (response->>'timestamp')::time AS "timestamp",
    (response->>'count')::bigint AS "count"
FROM zdb._all_indices_cat_request('count');

CREATE OR REPLACE VIEW zdb.cat_fielddata AS
SELECT
    url,
    (response->>'id') AS "id",
    (response->>'host') AS "host",
    (response->>'ip')::inet AS "ip",
    (response->>'node') AS "node",
    (response->>'field') AS "field",
    (response->>'size')::bigint AS "size"
FROM zdb._all_indices_cat_request('fielddata');

CREATE OR REPLACE VIEW zdb.cat_health AS
SELECT
    url,
    (response->>'epoch')::bigint AS "epoch",
    (response->>'timestamp')::time AS "timestamp",
    (response->>'cluster')::text AS "cluster",
    (response->>'status') AS "status",
    (response->>'node.total')::bigint AS "node.total",
    (response->>'node.data')::bigint AS "node.data",
    (response->>'shards')::int AS "shards",
    (response->>'pri')::int AS "pri",
    (response->>'relo')::int AS "relo",
    (response->>'init')::int AS "init",
    (response->>'unassign')::int AS "unassign",
    (response->>'pending_tasks')::int AS "pending_tasks",
    case when response->>'max_task_wait_time' <> '-' then ((response->>'max_task_wait_time')||'milliseconds')::interval else null end AS "max_task_wait_time",
    (response->>'active_shards_percent') AS "active_shards_percent"
FROM zdb._all_indices_cat_request('health');

CREATE OR REPLACE VIEW zdb.cat_indices AS
SELECT
    url,
    (response->>'health') AS "health",
    (response->>'status') AS "status",
    (select array_to_string(array_agg(alias), ',') from zdb.cat_aliases where index = response->>'index') as alias,
    (response->>'index') AS "index",
    (response->>'uuid') AS "uuid",
    (response->>'pri')::int AS "pri",
    (response->>'rep')::int AS "rep",
    (response->>'docs.count')::bigint AS "docs.count",
    (response->>'docs.deleted')::bigint AS "docs.deleted",
    (response->>'store.size')::bigint AS "store.size",
    (response->>'pri.store.size')::bigint AS "pri.store.size"
FROM zdb._all_indices_cat_request('indices')
WHERE response->>'index' IN (SELECT index from zdb.all_es_index_names() index);

CREATE OR REPLACE VIEW zdb.cat_master AS
SELECT
    url,
    (response->>'id') AS "id",
    (response->>'host') AS "host",
    (response->>'ip')::inet AS "ip",
    (response->>'node') AS "node"
FROM zdb._all_indices_cat_request('master');

CREATE OR REPLACE VIEW zdb.cat_nodeattrs AS
SELECT
    url,
    (response->>'node') AS "node",
    (response->>'host') AS "host",
    (response->>'ip')::inet AS "ip",
    (response->>'attr') AS "attr",
    (response->>'value') AS "value"
FROM zdb._all_indices_cat_request('nodeattrs');

CREATE OR REPLACE VIEW zdb.cat_nodes AS
SELECT
    url,
    (response->>'id')::text AS "id",
    (response->>'pid')::int AS "pid",
    (response->>'ip')::inet AS "ip",
    (response->>'port')::int AS "port",
    (response->>'http_address')::text AS "http_address",
    (response->>'version')::text AS "version",
    (response->>'build')::text AS "build",
    (response->>'jdk')::text AS "jdk",

    (response->>'disk.total')::text AS "disk.total",
    (response->>'disk.used')::text AS "disk.used",
    (response->>'disk.avail')::text AS "disk.avail",
    (response->>'disk.used_percent')::real AS "disk.used_percent",

    (response->>'heap.current')::text AS "heap.current",
    (response->>'heap.percent')::real AS "heap.percent",
    (response->>'heap.max')::text AS "heap.max",

    (response->>'ram.current')::text AS "ram.current",
    (response->>'ram.percent')::text AS "ram.percent",
    (response->>'ram.max')::text AS "ram.max",

    (response->>'file_desc.current')::int AS "file_desc.current",
    (response->>'file_desc.percent')::real AS "file_desc.percent",
    (response->>'file_desc.max')::int AS "file_desc.max",

    (response->>'cpu')::real AS "cpu",
    (response->>'load.1m')::real AS "load.1m",
    (response->>'load.5m')::real AS "load.5m",
    (response->>'load.15m')::real AS "load.15m",
    (response->>'uptime')::text AS "uptime",

    (response->>'node.role')::text AS "node.role",
    (response->>'master') = '*' AS "master",
    (response->>'name')::text AS "name",
    (response->>'completion.size')::text AS "completion.size",

    (response->>'fielddata.memory_size')::text AS "fielddata.memory_size",
    (response->>'fielddata.evictions')::int AS "fielddata.evictions",

    (response->>'query_cache.memory.size')::text AS "query_cache.memory.size",
    (response->>'query_cache.evictions')::int AS "query_cache.evictions",

    (response->>'request_cache.memory_size')::text AS "request_cache.memory_size",
    (response->>'request_cache.evictions')::int AS "request_cache.evictions",
    (response->>'request_cache.hit_count')::int AS "request_cache.hit_count",
    (response->>'request_cache.miss_count')::int AS "request_cache.miss_count",

    (response->>'flush.total')::int AS "flush.total",
    ((response->>'flush.total_time')::text||' milliseconds')::interval AS "flush.total_time",

    (response->>'get.current')::int AS "get.current",
    ((response->>'get.time')::text::text||' milliseconds')::interval AS "get.time",
    (response->>'get.total')::bigint AS "get.total",
    ((response->>'get.exists_time')::text||' milliseconds')::interval AS "get.exists_time",
    (response->>'get.exists_total')::bigint AS "get.exists_total",
    ((response->>'get.missing_time')::text||' milliseconds')::interval AS "get.missing_time",
    (response->>'get.missing_total')::bigint AS "get.missing_total",

    (response->>'indexing.delete_current')::bigint AS "indexing.delete_current",
    ((response->>'indexing.delete_time')::text||' milliseconds')::interval AS "indexing.delete_time",
    (response->>'indexing.delete_total')::bigint AS "indexing.delete_total",
    (response->>'indexing.index_current')::bigint AS "indexing.index_current",
    ((response->>'indexing.index_time')::text||' milliseconds')::interval AS "indexing.index_time",
    (response->>'indexing.index_total')::bigint AS "indexing.index_total",
    (response->>'indexing.index_failed')::bigint AS "indexing.index_failed",

    (response->>'merges.current')::bigint AS "merges.current",
    (response->>'merges.current.docs')::bigint AS "merges.current.docs",
    (response->>'merges.current.size')::text AS "merges.current.size",
    (response->>'merges.total')::bigint AS "merges.total",
    (response->>'merges.total_docs')::bigint AS "merges.total_docs",
    (response->>'merges.total_size')::text AS "merges.total_size",
    ((response->>'merges.total_time')::text||' milliseconds')::interval AS "merges.total_time",

    (response->>'refresh.total')::bigint AS "refresh.total",
    ((response->>'refresh.time')::text::text||' milliseconds')::interval AS "refresh.time",
    (response->>'refresh.listeners')::text AS "refresh.listeners",

    (response->>'script.compilations')::bigint AS "script.compilations",
    (response->>'script.cache_evictions')::bigint AS "script.cache_evictions",

    (response->>'search.fetch_current')::bigint AS "search.fetch_current",
    ((response->>'search.fetch_time')::text||' milliseconds')::interval AS "search.fetch_time",
    (response->>'search.fetch_total')::bigint AS "search.fetch_total",
    (response->>'search.open_contexts')::bigint AS "search.open_contexts",
    (response->>'search.query_current')::bigint AS "search.query_current",
    ((response->>'search.query_time')::text||' milliseconds')::interval AS "search.query_time",
    (response->>'search.query_total')::text AS "search.query_total",

    (response->>'search.scroll_current')::bigint AS "search.scroll_current",
    ((response->>'search.scroll_time')::bigint/1000 ||' milliseconds')::interval AS "search.scroll_time",
    (response->>'search.scroll_total')::bigint AS "search.scroll_total",

    (response->>'segments.count')::bigint AS "segments.count",
    (response->>'segments.memory')::text AS "segments.memory",
    (response->>'segments.index_writer_memory')::text AS "segments.index_writer_memory",
    (response->>'segments.version_map_memory')::text AS "segments.version_map_memory",
    (response->>'segments.fixed_bitset_memory')::text AS "segments.fixed_bitset_memory",

    (response->>'suggest.current')::bigint AS "suggest.current",
    ((response->>'suggest.time')::text||' milliseconds')::interval AS "suggest.time",
    (response->>'suggest.total')::bigint AS "suggest.total"
FROM zdb._all_indices_cat_request('nodes');

CREATE OR REPLACE VIEW zdb.cat_pending_tasks AS
SELECT
    url,
    (response->>'insertOrder')::int AS "insertOrder",
    ((response->>'timeInQueue')||' milliseconds')::interval AS "timeInQueue",
    (response->>'priority') AS "priority",
    (response->>'source') AS "source"
FROM zdb._all_indices_cat_request('pending_tasks');

CREATE OR REPLACE VIEW zdb.cat_plugins AS
SELECT
    url,
    (response->>'name') AS "name",
    (response->>'component') AS "component",
    (response->>'version') AS "version",
    (response->>'description') AS "description"
FROM zdb._all_indices_cat_request('plugins');

CREATE OR REPLACE VIEW zdb.cat_thread_pool AS
SELECT
    url,
    (response->>'ip')::inet AS "ip",
    (response->>'max')::int AS "max",
    (response->>'min')::int AS "min",
    (response->>'pid')::int AS "pid",
    (response->>'host')::text AS "host",
    (response->>'name')::text AS "name",
    (response->>'port')::int AS "port",
    (response->>'size')::int AS "size",
    (response->>'type')::text AS "type",
    (response->>'queue')::int AS "queue",
    (response->>'active')::int AS "active",
    (response->>'largest')::int AS "largest",
    (response->>'node_id')::text AS "node_id",
    (response->>'rejected')::int AS "rejected",
    (response->>'completed')::bigint AS "completed",
    (response->>'node_name')::text AS "node_name",
    (response->>'keep_alive')::text AS "keep_alive",
    (response->>'queue_size')::int AS "queue_size",
    (response->>'ephemeral_node_id')::text AS "ephemeral_node_id"
FROM zdb._all_indices_cat_request('thread_pool');

CREATE OR REPLACE VIEW zdb.cat_shards AS
SELECT
    url,
    (response->>'id')::text AS "id",
    (response->>'ip')::inet AS "ip",
    (response->>'docs')::bigint AS "docs",
    (response->>'node')::text AS "node",
    (select array_to_string(array_agg(alias), ',') from zdb.cat_aliases where index = response->>'index') as alias,
    (response->>'index')::text AS "index",
    (response->>'shard')::int AS "shard",
    (response->>'state')::text AS "state",
    (response->>'store')::bigint AS "store",
    (response->>'prirep')::text AS "prirep",
    (response->>'sync_id')::text AS "sync_id",
    (response->>'completion.size')::bigint AS "completion.size",
    (response->>'fielddata.evictions')::int AS "fielddata.evictions",
    (response->>'fielddata.memory_size')::bigint AS "fielddata.memory_size",
    (response->>'flush.total')::bigint AS "flush.total",
    ((response->>'flush.total_time')||'milliseconds')::interval AS "flush.total_time",
    (response->>'get.current')::int AS "get.current",
    ((response->>'get.exists_time')||'milliseconds')::interval AS "get.exists_time",
    (response->>'get.exists_total')::bigint AS "get.exists_total",
    ((response->>'get.missing_time')||'milliseconds')::interval AS "get.missing_time",
    (response->>'get.missing_total')::bigint AS "get.missing_total",
    ((response->>'get.time')||'milliseconds')::interval AS "get.time",
    (response->>'get.total')::bigint AS "get.total",
    (response->>'indexing.delete_current')::int AS "indexing.delete_current",
    ((response->>'indexing.delete_time')||'milliseconds')::interval AS "indexing.delete_time",
    (response->>'indexing.delete_total')::bigint AS "indexing.delete_total",
    (response->>'indexing.index_current')::int AS "indexing.index_current",
    (response->>'indexing.index_failed')::bigint AS "indexing.index_failed",
    ((response->>'indexing.index_time')||'milliseconds')::interval AS "indexing.index_time",
    (response->>'indexing.index_total')::bigint AS "indexing.index_total",
    (response->>'merges.current')::int AS "merges.current",
    (response->>'merges.current_docs')::int AS "merges.current_docs",
    (response->>'merges.current_size')::bigint AS "merges.current_size",
    (response->>'merges.total')::bigint AS "merges.total",
    (response->>'merges.total_docs')::bigint AS "merges.total_docs",
    (response->>'merges.total_size')::bigint AS "merges.total_size",
    ((response->>'merges.total_time')||'milliseconds')::interval AS "merges.total_time",
    (response->>'query_cache.evictions')::int AS "query_cache.evictions",
    (response->>'query_cache.memory_size')::bigint AS "query_cache.memory_size",
    (response->>'recoverysource.type')::text AS "recoverysource.type",
    (response->>'refresh.listeners')::int AS "refresh.listeners",
    ((response->>'refresh.time')||'milliseconds')::interval AS "refresh.time",
    (response->>'refresh.total')::bigint AS "refresh.total",
    (response->>'search.fetch_current')::int AS "search.fetch_current",
    ((response->>'search.fetch_time')||'milliseconds')::interval AS "search.fetch_time",
    (response->>'search.fetch_total')::bigint AS "search.fetch_total",
    (response->>'search.open_contexts')::int AS "search.open_contexts",
    (response->>'search.query_current')::int AS "search.query_current",
    ((response->>'search.query_time')||'milliseconds')::interval AS "search.query_time",
    (response->>'search.query_total')::bigint AS "search.query_total",
    (response->>'search.scroll_current')::int AS "search.scroll_current",
    ((response->>'search.scroll_time')::bigint / 1000 ||'milliseconds')::interval AS "search.scroll_time",
    (response->>'search.scroll_total')::bigint AS "search.scroll_total",
    (response->>'segments.count')::int AS "segments.count",
    (response->>'segments.fixed_bitset_memory')::bigint AS "segments.fixed_bitset_memory",
    (response->>'segments.index_writer_memory')::bigint AS "segments.index_writer_memory",
    (response->>'segments.memory')::bigint AS "segments.memory",
    (response->>'segments.version_map_memory')::bigint AS "segments.version_map_memory",
    (response->>'unassigned.at')::text AS "unassigned.at",
    (response->>'unassigned.details')::text AS "unassigned.details",
    (response->>'unassigned.for')::text AS "unassigned.for",
    (response->>'unassigned.reason')::text AS "unassigned.reason",
    (response->>'warmer.current')::int AS "warmer.current",
    (response->>'warmer.total')::bigint AS "warmer.total",
    ((response->>'warmer.total_time')||'milliseconds')::interval AS "warmer.total_time"
FROM zdb._all_indices_cat_request('shards')
WHERE response->>'index' IN (SELECT index from zdb.all_es_index_names() index);

CREATE OR REPLACE VIEW zdb.cat_segments AS
SELECT
    url,
    (response->>'id')::text AS "id",
    (response->>'ip')::inet AS "ip",
    (response->>'size')::bigint AS "size",
    (select array_to_string(array_agg(alias), ',') from zdb.cat_aliases where index = response->>'index') as alias,
    (response->>'index')::text AS "index",
    (response->>'shard')::int AS "shard",
    (response->>'prirep')::text AS "prirep",
    (response->>'segment')::text AS "segment",
    (response->>'version')::text AS "version",
    (response->>'compound')::boolean AS "compound",
    (response->>'committed')::boolean AS "committed",
    (response->>'docs.count')::bigint AS "docs.count",
    (response->>'generation')::int AS "generation",
    (response->>'searchable')::boolean AS "searchable",
    (response->>'size.memory')::bigint AS "size.memory",
    (response->>'docs.deleted')::bigint AS "docs.deleted"
FROM zdb._all_indices_cat_request('segments')
WHERE response->>'index' IN (SELECT index from zdb.all_es_index_names() index);




--
-- sql/_support-views.sql
--
--
-- a view to get quick stats about all indexes
--
CREATE OR REPLACE VIEW zdb.index_stats AS
WITH stats AS (
    SELECT indrelid :: REGCLASS                                          table_name,
           indexrelid::regclass                                          pg_index_name,
           zdb.index_name(indexrelid)                                    es_index_name,
           zdb.index_url(indexrelid)                                     url,
           zdb.request(indexrelid, '_stats', 'GET', NULL, true)::json    stats,
           zdb.request(indexrelid, '_settings', 'GET', NULL, true)::json settings
    FROM pg_index,
         pg_class
    where pg_class.oid = pg_index.indexrelid
      and relam = (select oid from pg_am where amname = 'zombodb')
)
SELECT (select array_to_string(array_agg(alias), ',') from zdb.cat_aliases where index = es_index_name) as alias,
       es_index_name,
       url,
       table_name,
       pg_index_name,
       stats -> '_all' -> 'primaries' -> 'docs' -> 'count'                                              AS es_docs,
       pg_size_pretty((stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8)          AS es_size,
       (stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8                          AS es_size_bytes,
       (SELECT reltuples::int8 FROM pg_class WHERE oid = table_name)                                    AS pg_docs_estimate,
       pg_size_pretty(pg_total_relation_size(table_name))                                               AS pg_size,
       pg_total_relation_size(table_name)                                                               AS pg_size_bytes,
       stats -> '_shards' -> 'total'                                                                    AS shards,
       settings -> es_index_name -> 'settings' -> 'index' ->> 'number_of_replicas'                      AS replicas,
       (zdb.request(pg_index_name, '_count', 'GET', NULL, true) :: JSON) -> 'count'                     AS doc_count,
       coalesce(json_array_length((zdb.request(pg_index_name, '_doc/zdb_aborted_xids', 'GET', NULL, true) :: JSON) ->
                                  '_source' -> 'zdb_aborted_xids'), 0)                               AS aborted_xids
FROM stats;



--
-- sql/_join-support.sql
--
CREATE OR REPLACE FUNCTION dsl.join(left_field name, index regclass, right_field name, query zdbquery,
                                    size int DEFAULT 0) RETURNS zdbquery
    PARALLEL SAFE STABLE
    LANGUAGE plpgsql AS
$$
BEGIN
    IF size > 0 THEN
        /* if we have a size limit, then limit to the top matching hits */
        RETURN dsl.bool(dsl.filter(
                dsl.terms(left_field, VARIADIC (SELECT coalesce(array_agg(source ->> right_field), ARRAY []::text[])
                                                FROM zdb.top_hits(index, ARRAY [right_field], query, size)))));
    ELSE
        /* otherwise, return all the matching terms */
        RETURN dsl.bool(dsl.filter(dsl.terms(left_field, VARIADIC zdb.terms_array(index, right_field, query))));
    END IF;
END ;
$$;



--
-- sql/highlighting_es_highlighting.generated.sql
--
CREATE TYPE pg_catalog.highlighttype AS ENUM (
'unified',
'plain',
'fvh'
);
CREATE TYPE pg_catalog.fragmentertype AS ENUM (
'simple',
'span'
);
CREATE TYPE pg_catalog.encodertype AS ENUM (
'default',
'html'
);
CREATE TYPE pg_catalog.boundaryscannertype AS ENUM (
'chars',
'sentence',
'word'
);
-- ./src/highlighting/es_highlighting.rs:43:0
CREATE OR REPLACE FUNCTION zdb."highlight"("highlight_type" HighlightType DEFAULT NULL, "require_field_match" bool DEFAULT 'false', "number_of_fragments" integer DEFAULT NULL, "highlight_query" ZDBQuery DEFAULT NULL, "pre_tags" text[] DEFAULT NULL, "post_tags" text[] DEFAULT NULL, "tags_schema" text DEFAULT NULL, "no_match_size" integer DEFAULT NULL, "fragmenter" FragmenterType DEFAULT NULL, "fragment_size" integer DEFAULT NULL, "fragment_offset" integer DEFAULT NULL, "force_source" bool DEFAULT 'true', "encoder" EncoderType DEFAULT NULL, "boundary_scanner_locale" text DEFAULT NULL, "boundary_scan_max" integer DEFAULT NULL, "boundary_chars" text DEFAULT NULL, "phrase_limit" integer DEFAULT NULL, "matched_fields" bool DEFAULT NULL, "order" text DEFAULT NULL) RETURNS json IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_wrapper';
-- ./src/highlighting/es_highlighting.rs:136:0
CREATE OR REPLACE FUNCTION zdb."highlight"("ctid" tid, "field" text, "_highlight_definition" json DEFAULT zdb . highlight ()) RETURNS text[] IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_field_wrapper';
-- ./src/highlighting/es_highlighting.rs:165:0
CREATE OR REPLACE FUNCTION zdb."want_highlight"("query" ZDBQuery, "field" text, "highlight_definition" json DEFAULT zdb . highlight ()) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'want_highlight_wrapper';



--
-- sql/highlighting_query_highlighter.generated.sql
--
-- ./src/highlighting/query_highlighter.rs:339:0
CREATE OR REPLACE FUNCTION zdb."highlight_document"("index" regclass, "document" JsonB, "query_string" text) RETURNS TABLE ("field_name" text, "array_index" integer, "term" text, "type" text, "position" integer, "start_offset" bigint, "end_offset" bigint, "query_clause" text) STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'highlight_document_wrapper';



--
-- sql/zql_dsl_mod.generated.sql
--
-- ./src/zql/dsl/mod.rs:16:0
CREATE OR REPLACE FUNCTION zdb."dump_query"("index" regclass, "query" ZDBQuery) RETURNS text IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'dump_query_wrapper';
-- ./src/zql/dsl/mod.rs:22:0
CREATE OR REPLACE FUNCTION zdb."debug_query"("index" regclass, "query" text) RETURNS TABLE ("normalized_query" text, "used_fields" text[], "ast" text) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'debug_query_wrapper';



--
-- sql/elasticsearch_suggest_term.generated.sql
--
-- ./src/elasticsearch/suggest_term.rs:87:0
CREATE OR REPLACE FUNCTION zdb."suggest_terms"("index" regclass, "field_name" text, "suggest" text, "query" ZDBQuery) RETURNS TABLE ("term" text, "offset" bigint, "length" bigint, "suggestion" text, "score" double precision, "frequency" bigint) IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'suggest_terms_wrapper';



--
-- sql/query_dsl_zdb.generated.sql
--
CREATE SCHEMA IF NOT EXISTS "dsl";
-- ./src/query_dsl/zdb.rs:9:4
CREATE OR REPLACE FUNCTION dsl."zdb"("index" regclass, "input" text) RETURNS ZDBQuery IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_wrapper';
-- ./src/query_dsl/zdb.rs:29:4
CREATE OR REPLACE FUNCTION dsl."link_options"("options" text[], "query" ZDBQuery) RETURNS ZDBQuery IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'link_options_wrapper';



--
-- sql/elasticsearch_aggregates_builders_terms.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/terms.rs:11:0
CREATE OR REPLACE FUNCTION zdb."terms_agg"("aggregate_name" text, "field" text, "size_limit" integer, "order_by" TermsOrderBy, "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'terms_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_metrics.generated.sql
--
CREATE TYPE zdb.ttesttype AS ENUM (
'Paired',
'Homoscedastic',
'Heteroscedastic'
);
-- ./src/elasticsearch/aggregates/builders/metrics.rs:36:0
CREATE OR REPLACE FUNCTION zdb."sum_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sum_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:51:0
CREATE OR REPLACE FUNCTION zdb."sum_agg"("aggregate_name" text, "field" text, "missing" double precision) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sum_agg_missing_float_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:67:0
CREATE OR REPLACE FUNCTION zdb."sum_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sum_agg_missing_int_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:84:0
CREATE OR REPLACE FUNCTION zdb."avg_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'avg_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:99:0
CREATE OR REPLACE FUNCTION zdb."avg_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'avg_agg_missing_int_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:115:0
CREATE OR REPLACE FUNCTION zdb."avg_agg"("aggregate_name" text, "field" text, "missing" double precision) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'avg_agg_missing_float_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:132:0
CREATE OR REPLACE FUNCTION zdb."min_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'min_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:147:0
CREATE OR REPLACE FUNCTION zdb."min_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'min_agg_missing_int_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:163:0
CREATE OR REPLACE FUNCTION zdb."min_agg"("aggregate_name" text, "field" text, "missing" double precision) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'min_agg_missing_float_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:180:0
CREATE OR REPLACE FUNCTION zdb."max_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'max_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:195:0
CREATE OR REPLACE FUNCTION zdb."max_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'max_agg_missing_int_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:211:0
CREATE OR REPLACE FUNCTION zdb."max_agg"("aggregate_name" text, "field" text, "missing" double precision) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'max_agg_missing_float_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:228:0
CREATE OR REPLACE FUNCTION zdb."stats_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'stats_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:243:0
CREATE OR REPLACE FUNCTION zdb."stats_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'stats_agg_missing_int_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:259:0
CREATE OR REPLACE FUNCTION zdb."stats_agg"("aggregate_name" text, "field" text, "missing" double precision) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'stats_agg_missing_float_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:276:0
CREATE OR REPLACE FUNCTION zdb."cardinality_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'cardinality_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:291:0
CREATE OR REPLACE FUNCTION zdb."cardinality_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'cardinality_agg_missing_int_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:307:0
CREATE OR REPLACE FUNCTION zdb."cardinality_agg"("aggregate_name" text, "field" text, "missing" double precision) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'cardinality_agg_missing_float_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:324:0
CREATE OR REPLACE FUNCTION zdb."extended_stats_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'extended_stats_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:339:0
CREATE OR REPLACE FUNCTION zdb."extended_stats_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'extended_stats_agg_missing_int_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:355:0
CREATE OR REPLACE FUNCTION zdb."extended_stats_agg"("aggregate_name" text, "field" text, "missing" double precision) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'extended_stats_agg_missing_float_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:372:0
CREATE OR REPLACE FUNCTION zdb."matrix_stats_agg"("aggregate_name" text, "field" text[]) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'matrix_stats_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:386:0
CREATE OR REPLACE FUNCTION zdb."matrix_stats_agg"("aggregate_name" text, "field" text[], "missing_field" text, "missing_value" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'matrix_stats_agg_missing_i64_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:408:0
CREATE OR REPLACE FUNCTION zdb."geo_bounds_agg"("aggregate_name" text, "field" text, "wrap_longitude" bool) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'geo_bounds_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:425:0
CREATE OR REPLACE FUNCTION zdb."value_count_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'value_count_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:441:0
CREATE OR REPLACE FUNCTION zdb."boxplot_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'boxplot_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:456:0
CREATE OR REPLACE FUNCTION zdb."boxplot_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'boxplot_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:472:0
CREATE OR REPLACE FUNCTION zdb."boxplot_agg"("aggregate_name" text, "field" text, "compression" bigint, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'boxplot_compression_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:495:0
CREATE OR REPLACE FUNCTION zdb."geo_centroid_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'geo_centroid_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:511:0
CREATE OR REPLACE FUNCTION zdb."median_absolute_deviation_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'median_absolute_deviation_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:527:0
CREATE OR REPLACE FUNCTION zdb."median_absolute_deviation_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'median_absolute_deviation_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:543:0
CREATE OR REPLACE FUNCTION zdb."median_absolute_deviation_agg"("aggregate_name" text, "field" text, "compression" bigint, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'median_absolute_deviation_compression_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:566:0
CREATE OR REPLACE FUNCTION zdb."percentiles_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:581:0
CREATE OR REPLACE FUNCTION zdb."percentiles"("aggregate_name" text, "field" text, "percents" double precision[]) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_precents_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:597:0
CREATE OR REPLACE FUNCTION zdb."percentiles"("aggregate_name" text, "field" text, "keyed" bool) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_keyed_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:613:0
CREATE OR REPLACE FUNCTION zdb."percentiles_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:629:0
CREATE OR REPLACE FUNCTION zdb."percentiles_agg"("aggregate_name" text, "field" text, "percents" double precision[], "keyed" bool) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_precent_keyed_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:651:0
CREATE OR REPLACE FUNCTION zdb."percentiles_agg"("aggregate_name" text, "field" text, "precents" double precision[], "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_precent_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:673:0
CREATE OR REPLACE FUNCTION zdb."percentiles_agg"("aggregate_name" text, "field" text, "keyed" bool, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_keyed_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:695:0
CREATE OR REPLACE FUNCTION zdb."percentiles_agg"("aggregate_name" text, "field" text, "percents" double precision[], "keyed" bool, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'percentiles_precents_keyed_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:720:0
CREATE OR REPLACE FUNCTION zdb."string_stats_agg"("aggregate_name" text, "field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'string_stats_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:737:0
CREATE OR REPLACE FUNCTION zdb."string_stats_agg"("aggregate_name" text, "field" text, "show_distribution" bool) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'string_stats_char_distribution_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:757:0
CREATE OR REPLACE FUNCTION zdb."string_stats_agg"("aggregate_name" text, "field" text, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'string_stats_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:773:0
CREATE OR REPLACE FUNCTION zdb."string_stats_agg"("aggregate_name" text, "field" text, "show_distribution" bool, "missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'string_stats_char_distribution_missing_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:796:0
CREATE OR REPLACE FUNCTION zdb."weighted_avg_agg"("aggregate_name" text, "field_value" text, "field_weight" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'weighted_avg_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:814:0
CREATE OR REPLACE FUNCTION zdb."weighted_avg_agg"("aggregate_name" text, "field_value" text, "field_weight" text, "value_missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'weighted_avg_missing_value_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:839:0
CREATE OR REPLACE FUNCTION zdb."weighted_avg_agg"("aggregate_name" text, "field_value" text, "field_weight" text, "value_missing" bigint, "weight_missing" bigint) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'weighted_avg_missings_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:866:0
CREATE OR REPLACE FUNCTION zdb."top_metrics_agg"("aggregate_name" text, "metric_field" text, "sort_type" SortDescriptor) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'top_metric_sort_desc_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:889:0
CREATE OR REPLACE FUNCTION zdb."top_metrics_agg"("aggregate_name" text, "metric_field" text) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'top_metric_score_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:907:0
CREATE OR REPLACE FUNCTION zdb."top_metrics_agg"("aggregate_name" text, "metric_field" text, "sort_type_lat_long" double precision[]) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'top_metric_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:930:0
CREATE OR REPLACE FUNCTION zdb."t_test_agg"("aggregate_name" text, "fields" text[], "t_type" TTestType) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 't_test_fields_agg_wrapper';
-- ./src/elasticsearch/aggregates/builders/metrics.rs:952:0
CREATE OR REPLACE FUNCTION zdb."t_test_agg"("aggregate_name" text, "fields" text[], "queries" ZDBQuery[], "t_type" TTestType) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 't_test_fields_queries_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_date_histogram.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/date_histogram.rs:12:0
CREATE OR REPLACE FUNCTION zdb."date_histogram_agg"("aggregate_name" text, "field" text, "calendar_interval" CalendarInterval DEFAULT NULL, "fixed_interval" text DEFAULT NULL, "time_zone" text DEFAULT '+00:00', "format" text DEFAULT 'yyyy-MM-dd', "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'date_histogram_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_filter.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/filter.rs:13:0
CREATE OR REPLACE FUNCTION zdb."filter_agg"("index" regclass, "aggregate_name" text, "filter" ZDBQuery, "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'filter_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_filters.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/filters.rs:13:0
CREATE OR REPLACE FUNCTION zdb."filters_agg"("index" regclass, "aggregate_name" text, "labels" text[], "filters" ZDBQuery[]) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'filters_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_range.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/range.rs:11:0
CREATE OR REPLACE FUNCTION zdb."range_agg"("aggregate_name" text, "field" text, "ranges" json[], "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'range_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_histogram.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/histogram.rs:23:0
CREATE OR REPLACE FUNCTION zdb."histogram_agg"("aggregate_name" text, "field" text, "interval" bigint, "min_count" bigint DEFAULT NULL, "keyed" bool DEFAULT NULL, "missing" bigint DEFAULT NULL, "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'histogram_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_adjacency_matrix.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/adjacency_matrix.rs:14:0
CREATE OR REPLACE FUNCTION zdb."adjacency_matrix_agg"("index" regclass, "aggregate_name" text, "labels" text[], "filters" ZDBQuery[]) RETURNS JsonB IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'adjacency_matrix_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_auto_date_histogram.generated.sql
--
CREATE TYPE zdb.intervals AS ENUM (
'Year',
'Month',
'Day',
'Hour',
'Minute',
'Second'
);
-- ./src/elasticsearch/aggregates/builders/auto_date_histogram.rs:32:0
CREATE OR REPLACE FUNCTION zdb."auto_date_histogram_agg"("aggregate_name" text, "field" text, "buckets" bigint, "format" text DEFAULT NULL, "minimum_interval" Intervals DEFAULT NULL, "missing" text DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'auto_date_histogram_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_childern.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/childern.rs:10:0
CREATE OR REPLACE FUNCTION zdb."children_agg"("aggregate_name" text, "join_type" text, "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'children_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_diversified_sampler.generated.sql
--
CREATE TYPE zdb.executionhint AS ENUM (
'Map',
'GlobalOrdinals',
'BytesHash'
);
-- ./src/elasticsearch/aggregates/builders/diversified_sampler.rs:28:0
CREATE OR REPLACE FUNCTION zdb."diversified_sampler_agg"("aggregate_name" text, "shard_size" bigint DEFAULT NULL, "max_docs_per_value" bigint DEFAULT NULL, "execution_hint" ExecutionHint DEFAULT NULL, "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'diversified_sampler_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_sampler.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/sampler.rs:10:0
CREATE OR REPLACE FUNCTION zdb."sampler_agg"("aggregate_name" text, "shard_size" bigint, "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'sampler_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_date_range.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/date_range.rs:23:0
CREATE OR REPLACE FUNCTION zdb."date_range_agg"("aggregate_name" text, "field" text, "format" text, "range" json[], "missing" text DEFAULT NULL, "keyed" bool DEFAULT NULL, "time_zone" text DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'date_range_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_geo_distance.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/geo_distance.rs:21:0
CREATE OR REPLACE FUNCTION zdb."geo_distance_agg"("aggregate_name" text, "field" text, "origin" text, "range" json[], "unit" text DEFAULT NULL, "keyed" bool DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'geo_distance_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_geohash_grid.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/geohash_grid.rs:23:0
CREATE OR REPLACE FUNCTION zdb."geohash_grid_agg"("aggregate_name" text, "field" text, "precision" smallint DEFAULT NULL, "bounds" text DEFAULT NULL, "size" bigint DEFAULT NULL, "shard_size" bigint DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'geohash_grid_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_geotile_grid.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/geotile_grid.rs:23:0
CREATE OR REPLACE FUNCTION zdb."geogrid_grid_agg"("aggregate_name" text, "field" text, "precision" smallint DEFAULT NULL, "bounds" text DEFAULT NULL, "size" bigint DEFAULT NULL, "shard_size" bigint DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'geogrid_grid_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_global.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/global.rs:10:0
CREATE OR REPLACE FUNCTION zdb."global_agg"("aggregate_name" text, "children" JsonB[] DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'global_agg_wrapper';



--
-- sql/elasticsearch_aggregates_builders_ip_range.generated.sql
--
-- ./src/elasticsearch/aggregates/builders/ip_range.rs:18:0
CREATE OR REPLACE FUNCTION zdb."ip_range_agg"("aggregate_name" text, "field" text, "range" json[], "keyed" bool DEFAULT NULL) RETURNS JsonB IMMUTABLE PARALLEL SAFE LANGUAGE c AS 'MODULE_PATHNAME', 'ip_range_agg_wrapper';



--
-- sql/_finalize.sql
--
CREATE OR REPLACE FUNCTION zdb.get_search_analyzer(index regclass, field text) RETURNS text
    IMMUTABLE STRICT PARALLEL SAFE
    LANGUAGE sql AS
$$
WITH properties AS (
    SELECT zdb.index_mapping(index) -> zdb.index_name(index) -> 'mappings' -> 'properties' ->
           field AS props)
SELECT COALESCE(props ->> 'search_analyzer', props ->> 'analyzer', 'standard')
FROM properties
LIMIT 1;

$$;

CREATE OR REPLACE FUNCTION zdb.get_index_analyzer(index regclass, field text) RETURNS text
    IMMUTABLE STRICT PARALLEL SAFE
    LANGUAGE sql AS
$$
WITH properties AS (
    SELECT zdb.index_mapping(index) -> zdb.index_name(index) -> 'mappings' -> 'properties' ->
           field AS props)
SELECT COALESCE(props ->> 'index_analyzer', props ->> 'analyzer', 'standard')
FROM properties
LIMIT 1;

$$;

CREATE OR REPLACE FUNCTION zdb.get_null_copy_to_fields(index regclass) RETURNS TABLE(field_name text, mapping jsonb)
    IMMUTABLE STRICT PARALLEL SAFE
    LANGUAGE sql AS
$$
WITH field_mapping AS (
    with properties as (
        select zdb.index_mapping(index) -> zdb.index_name(index) -> 'mappings' -> 'properties' as properties
    )
    SELECT key, properties.properties -> key as mapping
    FROM (
             SELECT jsonb_object_keys(properties.properties) as key
             FROM properties
         ) x,
         properties
)
SELECT *
FROM field_mapping
WHERE mapping ->> 'type' = 'text'
  and mapping ->> 'copy_to' is null
$$;

CREATE FUNCTION zdb.version() RETURNS TABLE (schema_version text, internal_version text) LANGUAGE sql AS $$
SELECT zdb.schema_version(), zdb.internal_version();
$$;




