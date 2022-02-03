DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.0.8 (c11743812075d06c3caa219343ae7f0ca6c1ee27)'
$$;

