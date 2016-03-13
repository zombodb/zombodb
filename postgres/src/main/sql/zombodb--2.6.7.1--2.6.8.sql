CREATE TABLE zdb_tokenizers (
  name text NOT NULL PRIMARY KEY,
  definition json NOT NULL
);
SELECT pg_catalog.pg_extension_config_dump('zdb_tokenizers', '');
CREATE OR REPLACE FUNCTION zdb_define_tokenizer(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb_tokenizers WHERE name = $1;
  INSERT INTO zdb_tokenizers(name, definition) VALUES ($1, $2);
$$;

INSERT INTO zdb_filters(name, definition, is_default) VALUES ('shingle_filter', '{
          "type": "shingle",
          "min_shingle_size": 2,
          "max_shingle_size": 2,
          "output_unigrams": true,
          "token_separator": "$"
        }', true);
INSERT INTO zdb_filters(name, definition, is_default) VALUES ('shingle_filter_search', '{
          "type": "shingle",
          "min_shingle_size": 2,
          "max_shingle_size": 2,
          "output_unigrams": false,
          "token_separator": "$"
        }', true);
INSERT INTO zdb_analyzers(name, definition, is_default) VALUES ('fulltext_with_shingles', '{
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "shingle_filter"
          ]
        }', true);
INSERT INTO zdb_analyzers(name, definition, is_default) VALUES ('fulltext_with_shingles_search', '{
          "type": "custom",
          "tokenizer": "standard",
          "filter": [
            "lowercase",
            "shingle_filter_search"
          ]
        }', true);
CREATE DOMAIN fulltext_with_shingles AS text;


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
        coalesce((x->>'key_as_string')::text, (x->>'key')::text),
        (x->>'doc_count')::int8
      FROM json_array_elements(buckets) x
    );
  ELSE
    RETURN QUERY (
      SELECT * FROM (SELECT NULL::text, missing LIMIT missing) x
      UNION ALL
      SELECT
        coalesce((x->>'key_as_string')::text, (x->>'key')::text),
        (x->>'doc_count')::int8
      FROM json_array_elements(buckets) x
    );
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION zdb_significant_terms(table_name regclass, fieldname text, is_nested boolean, stem text, query text, max_terms bigint) RETURNS SETOF zdb_significant_terms_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
  json_data json;
  type_oid oid;
  nested boolean;
  buckets json;
BEGIN
  type_oid := zdb_determine_index(table_name);
  json_data := zdb_internal_significant_terms(type_oid, CASE WHEN is_nested THEN format('#nested(%s)', fieldname) ELSE fieldname END, stem, query, max_terms);
  nested := (json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets') IS NOT NULL;

  IF nested THEN
    buckets := json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets';
  ELSE
    buckets := json_data->'aggregations'->fieldname->'buckets';
  END IF;

  RETURN QUERY (
    SELECT
      (x->>'key')::text,
      (x->>'doc_count')::int8,
      (x->>'score')::float8
    FROM json_array_elements(buckets) x
  );
END;
$$;

CREATE OR REPLACE FUNCTION zdb_suggest_terms(table_name regclass, fieldname text, base text, query text, max_terms int8) RETURNS SETOF zdb_suggest_terms_response LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
  type_oid oid;
  data json;
BEGIN
  type_oid := zdb_determine_index(table_name);
  data := zdb_internal_suggest_terms(type_oid, fieldname, base, query, max_terms);

  RETURN QUERY SELECT base, zdb_estimate_count(table_name, fieldname || ':("' || coalesce(case when trim(base) = '' then null else trim(base) end, 'null') || '") AND (' || coalesce(query, '') || ')')
               UNION ALL
               SELECT (x->>'text')::text,
                 (x->>'freq')::int8
               FROM json_array_elements(json_array_element(data->'suggest'->'suggestions', 0)->'options') x;
END;
$$;

CREATE OR REPLACE FUNCTION zdb_extract_table_row(table_name regclass, field_names text[], row_ctid tid) RETURNS json LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  is_view bool;
  pkey_column text;
  real_table_name regclass;
  row_data json;
BEGIN
  SELECT relkind = 'v' INTO is_view FROM pg_class WHERE oid = table_name;
  SELECT indrelid::regclass INTO real_table_name FROM pg_index WHERE indexrelid = zdb_determine_index(table_name);

  IF is_view THEN
    SELECT column_name
    INTO pkey_column
    FROM information_schema.key_column_usage
    WHERE (table_catalog || '.' || table_schema || '.' || information_schema.key_column_usage.table_name)::regclass = real_table_name;
  END IF;


  IF pkey_column IS NULL THEN
    /* just get what we can from the underlying table */
    EXECUTE format('SELECT row_to_json(x) FROM (SELECT %s, ctid FROM %s) x WHERE ctid = ''%s''',
                   CASE WHEN field_names IS NOT NULL THEN array_to_string(field_names, ',') ELSE (select array_to_string(array_agg(attname), ',') from pg_attribute where attrelid = real_table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0) END,
                   real_table_name,
                   row_ctid) INTO row_data;
  ELSE
    /* select out of the view */
    EXECUTE format('SELECT row_to_json(x) FROM (SELECT %s as _zdb_pkey, %s FROM %s) x WHERE _zdb_pkey = (SELECT %s FROM %s WHERE ctid = ''%s'')',
                   CASE WHEN field_names IS NOT NULL THEN array_to_string(field_names, ',') ELSE (select array_to_string(array_agg(attname), ',') from pg_attribute where attrelid = table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0) END,
                   pkey_column,
                   table_name,
                   pkey_column,
                   real_table_name,
                   row_ctid
    ) INTO row_data;
  END IF;

  RETURN row_data;
END;
$$;