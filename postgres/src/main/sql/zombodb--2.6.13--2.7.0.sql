DROP FUNCTION zdbtupledeletedtrigger();
CREATE OR REPLACE FUNCTION zdb_internal_update_mapping(index_oid oid) RETURNS void STRICT IMMUTABLE LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_update_mapping(table_name regclass) RETURNS void STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT zdb_internal_update_mapping(zdb_determine_index(table_name));
$$;
