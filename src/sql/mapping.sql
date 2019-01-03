--
-- PG to ES type mapping support
--

--
-- filter/analyzer/mapping support
--

CREATE TABLE filters (
  name text NOT NULL PRIMARY KEY,
  definition jsonb NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE char_filters (
  name text NOT NULL PRIMARY KEY,
  definition jsonb NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE analyzers (
  name text NOT NULL PRIMARY KEY,
  definition jsonb NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE normalizers (
  name text NOT NULL PRIMARY KEY,
  definition jsonb NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE mappings (
  table_name regclass NOT NULL,
  field_name text NOT NULL,
  definition jsonb NOT NULL,
  es_only boolean NOT NULL DEFAULT false,
  PRIMARY KEY (table_name, field_name)
);

CREATE TABLE type_mappings (
    type_name regtype NOT NULL PRIMARY KEY,
    definition jsonb NOT NULL,
    is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE tokenizers (
  name text NOT NULL PRIMARY KEY,
  definition jsonb NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('filters', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('char_filters', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('analyzers', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('normalizers', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('mappings', '');
SELECT pg_catalog.pg_extension_config_dump('tokenizers', '');
SELECT pg_catalog.pg_extension_config_dump('type_mappings', 'WHERE NOT is_default');

CREATE OR REPLACE FUNCTION define_filter(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.filters WHERE name = $1;
  INSERT INTO zdb.filters(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION define_char_filter(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.char_filters WHERE name = $1;
  INSERT INTO zdb.char_filters(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION define_analyzer(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.analyzers WHERE name = $1;
  INSERT INTO zdb.analyzers(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION define_normalizer(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.normalizers WHERE name = $1;
  INSERT INTO zdb.normalizers(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION define_field_mapping(table_name regclass, field_name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.mappings WHERE table_name = $1 AND field_name = $2;
  INSERT INTO zdb.mappings(table_name, field_name, definition) VALUES ($1, $2, $3);
$$;

CREATE OR REPLACE FUNCTION define_es_only_field(table_name regclass, field_name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.mappings WHERE table_name = $1 AND field_name = $2;
  INSERT INTO zdb.mappings(table_name, field_name, definition, es_only) VALUES ($1, $2, $3, true);
$$;

CREATE OR REPLACE FUNCTION define_type_mapping(type_name regtype, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.type_mappings WHERE type_name = $1;
  INSERT INTO zdb.type_mappings(type_name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION define_tokenizer(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.tokenizers WHERE name = $1;
  INSERT INTO zdb.tokenizers(name, definition) VALUES ($1, $2);
$$;

INSERT INTO filters(name, definition, is_default) VALUES (
  'zdb_truncate_to_fit', '{
    "type": "truncate",
    "length": 10922
  }', true);
INSERT INTO filters(name, definition, is_default) VALUES ('shingle_filter', '{
          "type": "shingle",
          "min_shingle_size": 2,
          "max_shingle_size": 2,
          "output_unigrams": true,
          "token_separator": "$"
        }', true);
INSERT INTO filters(name, definition, is_default) VALUES ('shingle_filter_search', '{
          "type": "shingle",
          "min_shingle_size": 2,
          "max_shingle_size": 2,
          "output_unigrams": false,
          "token_separator": "$"
        }', true);

INSERT INTO normalizers(name, definition, is_default) VALUES (
  'lowercase', '{
    "type": "custom",
    "char_filter": [],
    "filter": ["lowercase"]
  }', true);

INSERT INTO analyzers(name, definition, is_default) VALUES (
  'zdb_standard', '{
    "type": "standard",
    "filter": [ "zdb_truncate_to_fit", "lowercase" ]
  }', true);
INSERT INTO analyzers(name, definition, is_default) VALUES (
  'zdb_all_analyzer', '{
    "type": "standard",
    "filter": [ "zdb_truncate_to_fit", "lowercase" ]
  }', true);
INSERT INTO analyzers(name, definition, is_default) VALUES (
  'fulltext_with_shingles', '{
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "shingle_filter"
          ]
        }', true);
INSERT INTO analyzers(name, definition, is_default) VALUES (
  'fulltext_with_shingles_search', '{
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "shingle_filter_search"
          ]
        }', true);


CREATE DOMAIN fulltext AS text;
CREATE DOMAIN fulltext_with_shingles AS text;
CREATE DOMAIN zdb_standard AS text;

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'bytea', '{
    "type": "binary"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'boolean', '{
    "type": "boolean"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'smallint', '{
    "type": "short"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'integer', '{
    "type": "integer"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'bigint', '{
    "type": "long"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'real', '{
    "type": "float"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'double precision', '{
    "type": "double"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'character varying', '{
    "type": "keyword",
    "copy_to": "zdb_all",
    "ignore_above": 10922,
    "normalizer": "lowercase"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'text', '{
    "type": "text",
    "copy_to": "zdb_all",
    "fielddata": true,
    "analyzer": "zdb_standard"
  }', true);

--INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
--  'citext', '{
--    "type": "text",
--    "copy_to": "zdb_all",
--    "fielddata": true,
--    "analyzer": "zdb_standard"
--  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'time without time zone', '{
    "type": "date",
    "copy_to": "zdb_all",
    "format": "HH:mm:ss.SSSSSS"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'time with time zone', '{
    "type": "date",
    "copy_to": "zdb_all",
    "format": "HH:mm:ss.SSSSSSZZ"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'date', '{
    "type": "date",
    "copy_to": "zdb_all"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'timestamp without time zone', '{
    "type": "date",
    "copy_to": "zdb_all"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'timestamp with time zone', '{
    "type": "date",
    "copy_to": "zdb_all"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'json', '{
    "type": "nested",
    "include_in_parent": true
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'jsonb', '{
    "type": "nested",
    "include_in_parent": true
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'inet', '{
    "type": "ip",
    "copy_to": "zdb_all"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'fulltext', '{
    "type": "text",
    "copy_to": "zdb_all",
    "analyzer": "zdb_standard"
  }', true);

INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
  'fulltext_with_shingles', '{
    "type": "text",
    "copy_to": "zdb_all",
    "analyzer": "fulltext_with_shingles",
    "search_analyzer": "fulltext_with_shingles_search"
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
CREATE DOMAIN fingerprint AS text;
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
CREATE DOMAIN keyword AS character varying;
CREATE DOMAIN latvian AS text;
CREATE DOMAIN norwegian AS text;
CREATE DOMAIN persian AS text;
CREATE DOMAIN portuguese AS text;
CREATE DOMAIN romanian AS text;
CREATE DOMAIN russian AS text;
CREATE DOMAIN sorani AS text;
CREATE DOMAIN spanish AS text;
CREATE DOMAIN simple AS text;
CREATE DOMAIN standard AS text;
CREATE DOMAIN swedish AS text;
CREATE DOMAIN turkish AS text;
CREATE DOMAIN thai AS text;
CREATE DOMAIN whitespace AS text;

GRANT ALL ON analyzers TO PUBLIC;
GRANT ALL ON char_filters TO PUBLIC;
GRANT ALL ON filters TO PUBLIC;
GRANT ALL ON mappings TO PUBLIC;
GRANT ALL ON tokenizers TO PUBLIC;
GRANT ALL ON type_mappings TO PUBLIC;
GRANT ALL ON normalizers TO PUBLIC;
