CREATE TYPE zdb_multi_search_response AS (table_name regclass, user_identifier text, query text, total int8, score float4[], row_data json);

CREATE OR REPLACE FUNCTION zdb_id_to_ctid(id text) RETURNS tid LANGUAGE sql STRICT IMMUTABLE AS $$
SELECT ('(' || replace(id, '-', ',') || ')')::tid;
$$;

CREATE OR REPLACE FUNCTION zdb_extract_table_row(table_name regclass, row_ctid tid) RETURNS json LANGUAGE plpgsql STRICT IMMUTABLE AS $$
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
                   (select array_to_string(array_agg(attname), ',') from pg_attribute where attrelid = real_table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0),
                   real_table_name,
                   row_ctid) INTO row_data;
  ELSE
    /* select out of the view */
    EXECUTE format('SELECT row_to_json(x) FROM (SELECT %s FROM %s) x WHERE %s = (SELECT %s FROM %s WHERE ctid = ''%s'')',
                   (select array_to_string(array_agg(attname), ',') from pg_attribute where attrelid = table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0),
                   table_name,
                   pkey_column,
                   pkey_column,
                   real_table_name,
                   row_ctid
    ) INTO row_data;
  END IF;

  RETURN row_data;
END;
$$;

CREATE OR REPLACE FUNCTION zdb_internal_multi_search(table_names oid[], queries text[]) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_multi_search(table_names regclass[], user_identifiers text[], queries text[]) RETURNS SETOF zdb_multi_search_response LANGUAGE plpgsql STRICT IMMUTABLE AS $$
DECLARE
  response json;
  many integer;
BEGIN
  IF array_upper(table_names,1) <> array_upper(user_identifiers,1) OR array_upper(table_names,1) <> array_upper(queries,1) THEN
    RAISE EXCEPTION 'Arrays of table names, user_identifiers, and queries are not of the same length';
  END IF;

  response := zdb_internal_multi_search((SELECT array_agg(zdb_determine_index(unnest)) FROM unnest(table_names)), queries)->'responses';
  many := array_upper(table_names, 1);

  RETURN QUERY
  SELECT
    table_name,
    user_identifier,
    query,
    total,
    array_agg(score ORDER BY score DESC),
    json_agg(row_data ORDER BY score DESC)
  FROM (
         SELECT
           table_names[gs]                                                                                       AS table_name,
           user_identifiers[gs]                                                                                  AS user_identifier,
           queries[gs]                                                                                           AS query,
           (json_array_element(response, gs - 1) -> 'hits' ->>'total') :: INT8                                   AS total,
           (json_array_elements(json_array_element(response, gs - 1) -> 'hits' -> 'hits') ->>'_score') :: FLOAT4 AS score,
           zdb_extract_table_row(
               table_names[gs],
               zdb_id_to_ctid(json_array_elements(json_array_element(response, gs - 1) -> 'hits' -> 'hits') ->> '_id') :: tid
           )                                                                                                     AS row_data
         FROM generate_series(1, many) gs
       ) x
  GROUP BY 1, 2, 3, 4;
END;
$$;

CREATE OR REPLACE FUNCTION zdb_multi_search(table_names regclass[], user_identifier text[], query text) RETURNS SETOF zdb_multi_search_response LANGUAGE sql STRICT IMMUTABLE AS $$
  SELECT * FROM zdb_multi_search($1, $2, (SELECT array_agg($3) FROM unnest(table_names)));
$$;
