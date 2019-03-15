CREATE TABLE zdb.type_conversions (
  typeoid regtype NOT NULL PRIMARY KEY,
  funcoid regproc NOT NULL,
  is_default boolean DEFAULT false
);

SELECT pg_catalog.pg_extension_config_dump('type_conversions', 'WHERE NOT is_default');

CREATE OR REPLACE FUNCTION zdb.define_type_conversion(typeoid regtype, funcoid regproc) RETURNS void VOLATILE STRICT LANGUAGE sql AS $$
  DELETE FROM zdb.type_conversions WHERE typeoid = $1;
  INSERT INTO zdb.type_conversions(typeoid, funcoid) VALUES ($1, $2);
$$;


--
-- custom type conversions for some built-in postgres types
--

CREATE OR REPLACE FUNCTION zdb.point_to_json(point) RETURNS json PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
  SELECT to_json(ARRAY[$1[0], $1[1]]);
$$;

CREATE OR REPLACE FUNCTION zdb.point_array_to_json(point[]) RETURNS json PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
  SELECT json_agg(zdb.point_to_json(points)) FROM unnest($1) AS points;
$$;

INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default) VALUES ('point'::regtype, 'zdb.point_to_json'::regproc, true);
INSERT INTO zdb.type_conversions (typeoid, funcoid, is_default) VALUES ('point[]'::regtype, 'zdb.point_array_to_json'::regproc, true);