CREATE TYPE zdb_multi_search_response AS (table_name regclass, query text, total int8, ctid tid, score float4, row_data json);

CREATE OR REPLACE FUNCTION zdb_id_to_ctid(id text) RETURNS tid LANGUAGE sql STRICT IMMUTABLE AS $$
SELECT ('(' || replace(id, '-', ',') || ')')::tid;
$$;

CREATE OR REPLACE FUNCTION zdb_extract_table_row(table_name regclass, row_ctid tid) RETURNS json LANGUAGE plpgsql STRICT IMMUTABLE AS $$
DECLARE
   real_table_name regclass;
   row_data json;
BEGIN
   SELECT indrelid::regclass INTO real_table_name FROM pg_index WHERE indexrelid = zdb_determine_index(table_name);
   EXECUTE format('SELECT row_to_json(x) FROM (SELECT %s, ctid FROM %s) x WHERE ctid = ''%s''',
                  (select array_to_string(array_agg(attname), ',') from pg_attribute where attrelid = table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0),
                  real_table_name,
                  row_ctid) INTO row_data;
   RETURN row_data;
END;
$$;

CREATE OR REPLACE FUNCTION zdb_internal_multi_search(table_names oid[], queries text[]) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_multi_search(table_names regclass[], queries text[]) RETURNS SETOF zdb_multi_search_response LANGUAGE plpgsql STRICT IMMUTABLE AS $$
DECLARE
  response json;
  many integer;
BEGIN
  response := zdb_internal_multi_search((SELECT array_agg(zdb_determine_index(unnest)) FROM unnest(table_names)), queries)->'responses';
  many := array_upper(table_names, 1);

  RETURN QUERY SELECT
                 table_names[gs],
                 queries[gs],
                 (json_array_element(response, gs - 1)->'hits'->>'total')::int8,
                 zdb_id_to_ctid(json_array_elements(json_array_element(response, gs - 1)->'hits'->'hits')->> '_id')::tid,
                 (json_array_elements(json_array_element(response, gs-1)->'hits'->'hits')->>'_score')::float4,
                 zdb_extract_table_row(table_names[gs], zdb_id_to_ctid(json_array_elements(json_array_element(response, gs - 1)->'hits'->'hits')->>'_id')::tid)
                FROM generate_series(1, many) gs;
END;
$$;
CREATE OR REPLACE FUNCTION zdb_multi_search(table_names regclass[], query text) RETURNS SETOF zdb_multi_search_response LANGUAGE sql STRICT IMMUTABLE AS $$
  SELECT * FROM zdb_multi_search($1, (SELECT array_agg($2) FROM unnest(table_names)));
$$;
