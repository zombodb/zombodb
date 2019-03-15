CREATE OR REPLACE FUNCTION zdb.enable_postgis_support(during_create_extension bool DEFAULT false) RETURNS boolean VOLATILE LANGUAGE plpgsql AS $func$
DECLARE
  postgis_installed boolean := (SELECT count(*) > 0 FROM pg_extension WHERE extname = 'postgis');
  geojson_namespace text := (SELECT (SELECT nspname FROM pg_namespace WHERE oid = pronamespace) FROM pg_proc WHERE proname = 'st_asgeojson' limit 1);
BEGIN

  IF postgis_installed THEN
    RAISE WARNING '[zombodb] Installing support for PostGIS';

    -- casting functions
    EXECUTE format('create or replace function zdb.geometry_to_json(%I.geometry) returns json parallel safe immutable strict language sql as $$
          select %I.st_asgeojson(%I.st_transform($1, 4326))::json;
          $$;',
      geojson_namespace, geojson_namespace, geojson_namespace);
    EXECUTE format('create or replace function zdb.geography_to_json(%I.geography) returns json parallel safe immutable strict language sql as $$
          select %I.st_asgeojson(%I.st_transform($1::%I.geometry, 4326))::json;
          $$;',
      geojson_namespace, geojson_namespace, geojson_namespace, geojson_namespace);

    -- zdb type mappings
    EXECUTE format($$ SELECT zdb.define_type_mapping('%I.geometry'::regtype,  '{"type":"geo_shape"}'); $$, geojson_namespace);
    EXECUTE format($$ SELECT zdb.define_type_mapping('%I.geography'::regtype, '{"type":"geo_shape"}'); $$, geojson_namespace);

    -- zdb type conversions
    EXECUTE format($$ SELECT zdb.define_type_conversion('%I.geometry'::regtype, 'zdb.geometry_to_json'::regproc); $$, geojson_namespace);
    EXECUTE format($$ SELECT zdb.define_type_conversion('%I.geography'::regtype, 'zdb.geography_to_json'::regproc); $$, geojson_namespace);

    IF during_create_extension = false THEN
      EXECUTE 'ALTER EXTENSION zombodb ADD FUNCTION zdb.geometry_to_json';
      EXECUTE 'ALTER EXTENSION zombodb ADD FUNCTION zdb.geography_to_json';
    END IF;

  END IF;

  RETURN postgis_installed;
END;
$func$;

DO LANGUAGE plpgsql $$
  DECLARE
    postgis_installed boolean := (SELECT count(*) > 0 FROM pg_extension WHERE extname = 'postgis');
  BEGIN
    IF postgis_installed THEN
      PERFORM zdb.enable_postgis_support(true);
    END IF;
  END;
$$;
