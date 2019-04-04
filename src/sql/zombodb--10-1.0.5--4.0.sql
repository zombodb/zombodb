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
