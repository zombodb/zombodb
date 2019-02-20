CREATE TABLE zdb_pipelines (
  table_name regclass NOT NULL PRIMARY KEY,
  name text NOT NULL,
  definition json NOT NULL
);

SELECT pg_catalog.pg_extension_config_dump('zdb_pipelines', '');

CREATE OR REPLACE FUNCTION zdb_define_pipeline(table_name regclass, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
   DELETE FROM zdb_pipelines WHERE table_name = $1;
   INSERT INTO zdb_pipelines(table_name, name, definition) VALUES ($1, $1 || '-' || txid_current(), $2);
$$;

CREATE OR REPLACE FUNCTION zdb_delete_pipeline(table_name regclass) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb_pipelines WHERE table_name = $1;
$$;

