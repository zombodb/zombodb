DROP FUNCTION IF EXISTS zdb.schema_version();

-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.12 (3a56fb7db171826b2b6925fff808fc9c7ca52092)'
$$;

