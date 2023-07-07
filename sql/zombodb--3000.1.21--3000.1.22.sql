DROP FUNCTION IF EXISTS zdb.schema_version();
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.22 (e73bebc31d302b4fb1c10f90428adb5a8f384e8f)'
$$;

