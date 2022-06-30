DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.0.12 (ca389e2485d1cacbbd251161c2bda10fb5c55ffe)'
$$;

