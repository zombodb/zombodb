CREATE OR REPLACE FUNCTION zdb_highlight(table_name regclass, es_query text, where_clause text) RETURNS SETOF zdb_highlight_response LANGUAGE plpgsql STRICT VOLATILE AS $$
DECLARE
  type_oid oid;
  document_json json;
  json_data json;
  columns text;
BEGIN
  type_oid := zdb_determine_index(table_name);
  SELECT array_to_string(array_agg(attname), ',') FROM pg_attribute WHERE attrelid = table_name AND attname <> 'zdb' AND not attisdropped INTO columns;

  EXECUTE format('SELECT json_agg(data) FROM (SELECT row_to_json(the_table) AS data FROM (SELECT %s FROM %s) the_table WHERE %s) x', columns, table_name, where_clause) INTO document_json;
  json_data := zdb_internal_highlight(type_oid, to_json(es_query), document_json);

  RETURN QUERY (
    SELECT * FROM json_populate_recordset(null::zdb_highlight_response, json_data) AS highlight_data ORDER BY "primaryKey", "fieldName", "arrayIndex", "position"
  );
END;
$$;
CREATE OR REPLACE FUNCTION zdb_highlight(_table_name REGCLASS, es_query TEXT, where_clause TEXT, columns TEXT []) RETURNS SETOF zdb_highlight_response LANGUAGE plpgsql VOLATILE AS $$
DECLARE
  type_oid     OID;
  document_json json;
  json_data    JSON;
  columns_list TEXT;
BEGIN
  IF columns IS NULL OR columns = ARRAY[]::text[] THEN
    RETURN QUERY SELECT * FROM zdb_highlight(_table_name, es_query, where_clause);
  END IF;

  type_oid := zdb_determine_index(_table_name);

  SELECT array_to_string(array_agg(DISTINCT split_part(column_name, '.', 1)), ',') FROM (
                                                                                          select column_name from information_schema.key_column_usage where (table_schema, table_name) = (select (select nspname from pg_namespace where oid = relnamespace) as schema_name, relname as table_name from pg_class where oid = (select indrelid from pg_index where indexrelid = type_oid))
                                                                                          UNION
                                                                                          select unnest(columns)
                                                                                        ) x INTO columns_list;

  EXECUTE format('SELECT json_agg(data) FROM (SELECT row_to_json(the_table) AS data FROM (SELECT %s FROM %s) the_table WHERE %s) x', columns_list, _table_name, where_clause) INTO document_json;
  json_data := zdb_internal_highlight(type_oid, to_json(es_query), document_json);

  RETURN QUERY (
    SELECT *
    FROM json_populate_recordset(NULL :: zdb_highlight_response, json_data) AS highlight_data
    ORDER BY "primaryKey", "fieldName", "arrayIndex", "position"
  );
END;
$$;