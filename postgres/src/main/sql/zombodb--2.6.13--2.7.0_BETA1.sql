CREATE OR REPLACE FUNCTION rest_delete(url text) RETURNS json AS '$libdir/plugins/zombodb' language c;

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

    /*
     * -XDELETE the 'xact' type from all ZomboDB indexes.
     *
     * THERE'S NO GOING BACK FROM HERE
     */
    FOR r IN
        SELECT (zdb_get_url(indexrelid::regclass) || zdb_get_index_name(indexrelid::regclass) || '/xact') AS url
        FROM pg_index
        WHERE indclass[0] = (SELECT oid
                              FROM pg_opclass
                              WHERE opcmethod = (SELECT oid
                                                 FROM pg_am
                                                 WHERE amname = 'zombodb') AND opcname = 'zombodb_tid_ops')
    LOOP
        PERFORM rest_delete(r.url);
    END LOOP;
END;
$$;

DROP FUNCTION rest_delete(text);

DROP FUNCTION zdbtupledeletedtrigger();
CREATE OR REPLACE FUNCTION zdb_internal_update_mapping(index_oid oid) RETURNS void STRICT IMMUTABLE LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_update_mapping(table_name regclass) RETURNS void STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT zdb_internal_update_mapping(zdb_determine_index(table_name));
$$;
