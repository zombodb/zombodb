DROP FUNCTION IF EXISTS zdb.schema_version();
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.9 (6a66913bd53321ef1a1f46c98e51fdd48d7ea2f1)'
$$;

