CREATE OR REPLACE FUNCTION zdb_internal_dump_query(index_oid oid, user_query text) RETURNS text STRICT IMMUTABLE LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_dump_query(table_name regclass, user_query text) RETURNS text STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT zdb_internal_dump_query(zdb_determine_index(table_name), user_query);
$$;
