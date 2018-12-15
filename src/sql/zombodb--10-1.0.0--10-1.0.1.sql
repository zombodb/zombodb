CREATE TYPE dsl.es_sort_modes AS ENUM ('min', 'max', 'sum', 'avg', 'median');
CREATE TYPE dsl.es_sort_descriptor AS (
    field text,
    "order" dsl.es_sort_directions,
    mode dsl.es_sort_modes,
    nested_path text,
    nested_filter zdbquery
);

CREATE OR REPLACE FUNCTION dsl.sd(field text, "order" dsl.es_sort_directions, mode dsl.es_sort_modes DEFAULT NULL) RETURNS dsl.es_sort_descriptor PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ($1, $2, $3, null, null)::dsl.es_sort_descriptor;
$$;

CREATE OR REPLACE FUNCTION dsl.sd_nested(field text, "order" dsl.es_sort_directions, nested_path text, nested_filter zdbquery DEFAULT NULL, mode dsl.es_sort_modes DEFAULT NULL) RETURNS dsl.es_sort_descriptor PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ($1, $2, $5, $3, $4)::dsl.es_sort_descriptor;
$$;

CREATE OR REPLACE FUNCTION dsl.sort_many(query zdbquery, VARIADIC descriptors dsl.es_sort_descriptor[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    WITH sort_json AS (
        WITH descriptors AS (
            SELECT d.field, json_strip_nulls(json_build_object('order', d.order, 'mode', d.mode, 'nested_path', d.nested_path, 'nested_filter', d.nested_filter)) descriptor FROM unnest($2) d
        )
        SELECT json_agg(json_build_object(field, descriptor)) json FROM descriptors
    )
    SELECT zdb.set_query_property('sort_json', (SELECT json::jsonb::text FROM sort_json), $1);
$$;

CREATE OR REPLACE FUNCTION dsl.sort_direct(sort_json json, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT zdb.set_query_property('sort_json', $1::jsonb::text, $2);
$$;

CREATE OR REPLACE FUNCTION dsl.sort(sort_field text, sort_direction dsl.es_sort_directions, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT dsl.sort_many(query, dsl.sd(sort_field, sort_direction));
$$;
