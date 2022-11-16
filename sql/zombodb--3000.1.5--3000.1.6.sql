DROP FUNCTION IF EXISTS zdb.schema_version();
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.6 (51e9e116d5402cfc1d946ecbd134cac1d672186f)'
$$;

