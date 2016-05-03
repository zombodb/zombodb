DO LANGUAGE plpgsql $$
DECLARE
    r record;
BEGIN
    /*
     * delete exisitng ZomboDB triggers, if we have any
     */
    FOR r IN SELECT oid, * FROM pg_trigger WHERE tgname LIKE 'zzzzdb_tuple_sync_for_%'
    LOOP
        EXECUTE format('DELETE FROM pg_depend WHERE objid = %s AND deptype = ''i''', r.oid);
        EXECUTE format('DROP TRIGGER %s ON %s;', r.tgname, r.tgrelid::regclass);
    END LOOP;

END;
$$;

DROP FUNCTION zdbtupledeletedtrigger();
CREATE OR REPLACE FUNCTION zdb_internal_update_mapping(index_oid oid) RETURNS void STRICT IMMUTABLE LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_update_mapping(table_name regclass) RETURNS void STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT zdb_internal_update_mapping(zdb_determine_index(table_name));
$$;
