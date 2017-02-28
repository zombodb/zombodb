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
    WHERE format('%I.%I.%I', table_catalog, table_schema, information_schema.key_column_usage.table_name)::regclass = real_table_name;
  END IF;


  IF pkey_column IS NULL THEN
    /* just get what we can from the underlying table */
    EXECUTE format('SELECT row_to_json(x) FROM (SELECT "%s", ctid FROM %s) x WHERE ctid = ''%s''',
                   CASE WHEN field_names IS NOT NULL THEN array_to_string(field_names, '","') ELSE (select array_to_string(array_agg(attname), '","') from pg_attribute where attrelid = real_table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0) END,
                   real_table_name,
                   row_ctid) INTO row_data;
  ELSE
    /* select out of the view */
    EXECUTE format('SELECT row_to_json(x) FROM (SELECT %I as _zdb_pkey, "%s" FROM %s) x WHERE _zdb_pkey = (SELECT %I FROM %s WHERE ctid = ''%s'')',
                   pkey_column,
                   CASE WHEN field_names IS NOT NULL THEN array_to_string(field_names, '","') ELSE (select array_to_string(array_agg(attname), '","') from pg_attribute where attrelid = table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0) END,
                   table_name,
                   pkey_column,
                   real_table_name,
                   row_ctid
    ) INTO row_data;
  END IF;

  RETURN row_data;
END;
$$;
