ALTER TABLE mappings ADD COLUMN es_only boolean NOT NULL DEFAULT false;

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

CREATE OR REPLACE FUNCTION  define_field_mapping(table_name regclass, field_name name, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb.mappings WHERE table_name = $1 AND field_name = $2;
  INSERT INTO zdb.mappings(table_name, field_name, definition) VALUES ($1, $2, $3);
$$;

CREATE OR REPLACE FUNCTION define_es_only_field(table_name regclass, field_name name, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
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
