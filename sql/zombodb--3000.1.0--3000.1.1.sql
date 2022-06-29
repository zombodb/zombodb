DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.1 (3c6d5116f557b98e4011ccd401c5d6cecb73574d)'
$$;

