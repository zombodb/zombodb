DROP OPERATOR CLASS zombodb_ops USING zombodb CASCADE;
DROP OPERATOR ==>(json, text);

CREATE OR REPLACE FUNCTION zdb_determine_index(table_name regclass) RETURNS oid STRICT VOLATILE LANGUAGE plpgsql AS $$
DECLARE
  exp json;
  kind char;
  index_oid oid;
BEGIN
    SELECT relkind INTO kind FROM pg_class WHERE oid = table_name::oid;

    IF kind = 'r' THEN
      EXECUTE format('SET enable_seqscan TO OFF; EXPLAIN (FORMAT JSON) SELECT 1 FROM %s x WHERE zdb(''%s'', ctid) ==> '''' ', table_name, table_name) INTO exp;
    ELSE
      EXECUTE format('SET enable_seqscan TO OFF; EXPLAIN (FORMAT JSON) SELECT 1 FROM %s WHERE zdb ==> '''' ', table_name) INTO exp;
    END IF;

    IF (json_array_element(exp, 0)->'Plan'->'Plans')::text IS NOT NULL THEN
      RETURN oid FROM pg_class WHERE relname IN (SELECT json_array_elements(json_array_element(exp, 0)->'Plan'->'Plans')->>'Index Name' AS index_name)
                                 AND relam = (SELECT oid FROM pg_am WHERE amname = 'zombodb');
    END IF;

    index_oid := (json_array_element(exp, 0)->'Plan'->>'Index Name')::regclass::oid;
    IF index_oid IS NULL THEN
      RAISE EXCEPTION 'Unable to determine the index to use for %', table_name;
    END IF;
    RETURN index_oid;
END;
$$;

CREATE OPERATOR ==> (
    PROCEDURE = zdb_query_func,
    LEFTARG = json,
    RIGHTARG = text
);

CREATE OPERATOR CLASS zombodb_json_ops DEFAULT FOR TYPE json USING zombodb AS STORAGE json;

CREATE OR REPLACE FUNCTION zdb(table_name regclass, ctid tid) RETURNS tid LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb', 'zdb_table_ref_and_tid';
CREATE OR REPLACE FUNCTION zdb_tid_query_func(tid, text) RETURNS bool LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb' COST 10;

CREATE OPERATOR ==> (
    PROCEDURE = zdb_tid_query_func,
    RESTRICT = zdbsel,
    LEFTARG = tid,
    RIGHTARG = text,
    HASHES, MERGES
);

CREATE OPERATOR CLASS zombodb_tid_ops DEFAULT FOR TYPE tid USING zombodb AS
    OPERATOR 1 ==>(tid, text),
    FUNCTION 1 zdb_tid_query_func(tid, text),
    STORAGE json;