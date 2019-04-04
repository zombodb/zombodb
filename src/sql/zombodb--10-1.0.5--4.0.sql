CREATE OR REPLACE FUNCTION dsl.datetime_range(field text, lt timestamp with time zone DEFAULT NULL, gt timestamp with time zone DEFAULT NULL, lte timestamp with time zone DEFAULT NULL, gte timestamp with time zone DEFAULT NULL, boost real DEFAULT NULL) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('range', json_build_object(field,
            json_build_object(
                'lt', lt at time zone 'utc',
                'gt', gt at time zone 'utc',
                'lte', lte at time zone 'utc',
                'gte', gte at time zone 'utc'
            )
        )))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.geo_shape(field text, geojson_shape json, relation dsl.es_geo_shape_relation) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('geo_shape', json_build_object(field, json_build_object('shape', geojson_shape, 'relation', relation))))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.geo_bounding_box(field text, box box, type dsl.es_geo_bounding_box_type DEFAULT 'memory') RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('geo_bounding_box', json_build_object('type', type, field, json_build_object('left', (box[0])[0], 'top', (box[0])[1], 'right', (box[1])[0], 'bottom', (box[1])[1]))))::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.geo_polygon(field text, VARIADIC points point[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('geo_polygon', json_build_object(field, json_build_object('points', zdb.point_array_to_json(points)))))::zdbquery;
$$;