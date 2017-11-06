CREATE OR REPLACE FUNCTION zdb_internal_profile_query(index_oid oid, user_query text) RETURNS text STRICT IMMUTABLE LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_profile_query(table_name regclass, user_query text) RETURNS text STRICT IMMUTABLE LANGUAGE sql AS $$
  SELECT zdb_internal_profile_query(zdb_determine_index(table_name), user_query);
$$;

CREATE TABLE zdb_normalizers (
  name text NOT NULL PRIMARY KEY,
  definition json NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('zdb_normalizers', 'WHERE NOT is_default');
CREATE OR REPLACE FUNCTION zdb_define_normalizer(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb_normalizers WHERE name = $1;
  INSERT INTO zdb_normalizers(name, definition) VALUES ($1, $2);
$$;
INSERT INTO zdb_normalizers(name, definition, is_default) VALUES (
  'exact', '{
    "type": "custom",
    "char_filter": [],
    "filter": ["lowercase"]
  }', true);
