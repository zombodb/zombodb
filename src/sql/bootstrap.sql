--
-- function to alter the database so that the zdb.so library is loaded every time a session starts
--
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

GRANT ALL ON SCHEMA zdb TO PUBLIC;