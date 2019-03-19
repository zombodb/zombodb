--
-- mapping.sql
--
INSERT INTO type_mappings(type_name, definition, is_default) VALUES (
'point', '{
    "type": "geo_point"
}', true);



--
-- json-conversion-support.sql
--

CREATE TABLE zdb.type_conversions
(
  typeoid    regtype NOT NULL PRIMARY KEY,
  funcoid    regproc NOT NULL,
  is_default boolean DEFAULT false
);

SELECT pg_catalog.pg_extension_config_dump('type_conversions', 'WHERE NOT is_default');

CREATE OR REPLACE FUNCTION zdb.define_type_conversion(typeoid regtype, funcoid regproc) RETURNS void VOLATILE STRICT LANGUAGE sql AS
$$
  DELETE FROM zdb.type_conversions WHERE typeoid = $1; INSERT INTO zdb.type_conversions(typeoid, funcoid) VALUES ($1, $2);
$$;

GRANT ALL ON zdb.type_conversions TO PUBLIC;

--
-- custom type conversions for some built-in postgres types
--

CREATE OR REPLACE FUNCTION zdb.point_to_json(point) RETURNS json PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS
$$
  SELECT to_json(ARRAY [$1 [ 0], $1 [ 1]]);
$$;

CREATE OR REPLACE FUNCTION zdb.point_array_to_json(point[]) RETURNS json PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS
$$
  SELECT json_agg(zdb.point_to_json(points)) FROM unnest($1) AS points;
$$;

INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default) VALUES ('point'::regtype, 'zdb.point_to_json'::regproc, true);
INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default) VALUES ('point[]'::regtype, 'zdb.point_array_to_json'::regproc, true);


CREATE OR REPLACE FUNCTION zdb.bytea_to_json(bytea) RETURNS json PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
  SELECT to_json(encode($1, 'base64'));
$$;

INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default) VALUES ('bytea'::regtype, 'zdb.bytea_to_json'::regproc, true);

--
-- postgis-support.sql
--

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




--
-- query-dsl.sql
--

CREATE TYPE dsl.es_geo_shape_relation AS ENUM ('INTERSECTS', 'DISJOINT', 'WITHIN', 'CONTAINS');
CREATE OR REPLACE FUNCTION dsl.geo_shape(field text, geojson_shape json, relation dsl.es_geo_shape_relation) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
SELECT json_build_object('geo_shape', json_build_object(field, json_build_object('shape', geojson_shape, 'relation', relation)))::zdbquery;
$$;

CREATE TYPE dsl.es_geo_bounding_box_type AS ENUM ('indexed', 'memory');
CREATE OR REPLACE FUNCTION dsl.geo_bounding_box(field text, box box, type dsl.es_geo_bounding_box_type DEFAULT 'memory') RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
SELECT json_build_object('geo_bounding_box', json_build_object('type', type, field, json_build_object('left', (box[0])[0], 'top', (box[0])[1], 'right', (box[1])[0], 'bottom', (box[1])[1])))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.geo_polygon(field text, VARIADIC points point[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
SELECT json_build_object('geo_polygon', json_build_object(field, json_build_object('points', zdb.point_array_to_json(points))))::zdbquery;
$$;


