CREATE OR REPLACE FUNCTION zdb_determine_index(table_name regclass) RETURNS oid STRICT VOLATILE LANGUAGE plpgsql AS $$
DECLARE
  exp json;
  kind char;
  index_oid oid;
  namespace_oid oid;
BEGIN
  SELECT relkind, relnamespace INTO kind, namespace_oid FROM pg_class WHERE oid = table_name::oid;

  IF kind = 'r' THEN
    EXECUTE format('SET enable_seqscan TO OFF; EXPLAIN (FORMAT JSON) SELECT 1 FROM %s x WHERE zdb(''%s'', ctid) ==> '''' ', table_name, table_name) INTO exp;
  ELSE
    EXECUTE format('SET enable_seqscan TO OFF; EXPLAIN (FORMAT JSON) SELECT 1 FROM %s WHERE zdb ==> '''' ', table_name) INTO exp;
  END IF;

  IF (json_array_element(exp, 0)->'Plan'->'Plans')::text IS NOT NULL THEN
    RETURN oid FROM pg_class WHERE relname IN (SELECT unnest(regexp_matches(exp::text, '"Index Name":\s*"(.*)",*$', 'gn')))
    AND relam = (SELECT oid FROM pg_am WHERE amname = 'zombodb') AND relnamespace = namespace_oid;
  END IF;

  SELECT oid INTO index_oid FROM pg_class WHERE relname = (json_array_element(exp, 0)->'Plan'->>'Index Name') AND relnamespace = namespace_oid;
  IF index_oid IS NULL THEN
    RAISE EXCEPTION 'Unable to determine the index to use for %', table_name;
  END IF;
  RETURN index_oid;
END;
$$;
