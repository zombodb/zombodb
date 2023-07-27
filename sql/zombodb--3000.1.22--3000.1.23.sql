DROP FUNCTION IF EXISTS zdb.schema_version();
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.23 (cd694313cfe735840ffad911d205a77d6522db2f)'
$$;

