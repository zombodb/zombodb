DROP FUNCTION IF EXISTS zdb.schema_version();
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.21 (aa093cfb67f6e33fc6178fef5eccaa42d4b8782a)'
$$;

