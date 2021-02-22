DO LANGUAGE plpgsql $$
    DECLARE
        session_preload_libraries text;
    BEGIN
        session_preload_libraries = COALESCE (current_setting('session_preload_libraries'), '');
        IF (session_preload_libraries NOT LIKE '%zombodb.so%') THEN
            IF (session_preload_libraries = '') THEN
                session_preload_libraries = 'zombodb.so';
            ELSE
                session_preload_libraries = format('zombodb.so,%s', session_preload_libraries);
            END IF;

            EXECUTE format('ALTER DATABASE %I SET session_preload_libraries TO ''%s''', current_database(), session_preload_libraries);
        END IF;

    END;
$$;

-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '@DEFAULT_VERSION@ (@GIT_HASH@)'
$$;

CREATE SCHEMA dsl;

GRANT ALL ON SCHEMA zdb TO PUBLIC;
GRANT ALL ON SCHEMA dsl TO PUBLIC;
