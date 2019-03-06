CREATE EXTENSION zombodb;
CREATE SCHEMA postgis; CREATE EXTENSION postgis SCHEMA postgis;
ALTER DATABASE contrib_regression SET search_path = public, dsl;
SELECT zdb.enable_postgis_support();