DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.0.0 (e61edde2b23674b2d1d2e3cd9db88ea76a5ec0c9)'
$$;

