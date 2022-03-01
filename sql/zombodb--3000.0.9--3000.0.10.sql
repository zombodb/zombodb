DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.0.10 (52704a615d1e0a06f93afe955b904fafe87ce997)'
$$;

