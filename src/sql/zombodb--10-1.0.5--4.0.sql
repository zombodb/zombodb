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


--
-- emoji analyzer support
--
ALTER TABLE tokenizers ADD COLUMN is_default boolean DEFAULT false;
SELECT pg_catalog.pg_extension_config_dump('tokenizers', 'WHERE NOT is_default');

INSERT INTO tokenizers(name, definition, is_default)
VALUES ('emoji', '{
  "type": "pattern",
  "pattern": "([\\ud83c\\udf00-\\ud83d\\ude4f]|[\\ud83d\\ude80-\\ud83d\\udeff])",
  "group": 1
}', true);

INSERT INTO analyzers(name, definition, is_default)
VALUES ('emoji', '{
  "tokenizer": "emoji"
}', true);


--
-- nested aggregate changes
---

CREATE OR REPLACE FUNCTION zdb.extract_the_agg_data(index regclass, field text, response jsonb) RETURNS jsonb
    PARALLEL SAFE IMMUTABLE STRICT
    LANGUAGE sql AS
$$
SELECT CASE
           WHEN zdb.is_nested_field(index, field) THEN
               response -> 'aggregations' -> 'nested_agg' -> 'the_agg' -> 'filtered_agg'
           ELSE
               response -> 'aggregations' -> 'the_agg'
           END;
$$;


DROP FUNCTION zdb.significant_terms;
DROP FUNCTION zdb.internal_significant_terms;

CREATE OR REPLACE FUNCTION zdb.internal_significant_terms(index regclass, field text, query zdbquery, include text, size int, min_doc_count int) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_significant_terms';
CREATE OR REPLACE FUNCTION zdb.significant_terms(index regclass, field text, query zdbquery, include text DEFAULT '^.*', size int DEFAULT 10, min_doc_count int DEFAULT 3) RETURNS TABLE (term text, doc_count bigint, score numeric, bg_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_significant_terms(index, field, query, include, size, min_doc_count)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key', (entry->>'doc_count')::bigint, (entry->>'score')::numeric, (entry->>'bg_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;


DROP FUNCTION zdb.internal_histogram;
DROP FUNCTION zdb.histogram;
CREATE OR REPLACE FUNCTION zdb.internal_histogram(index regclass, field text, query zdbquery, "interval" float8, min_doc_count int) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_histogram';
CREATE OR REPLACE FUNCTION zdb.histogram(index regclass, field text, query zdbquery, "interval" float8, min_doc_count int DEFAULT 0) RETURNS TABLE (key numeric, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_histogram(index, field, query, "interval", min_doc_count)::jsonb;
BEGIN
    RETURN QUERY SELECT (entry->>'key')::numeric,
                        (entry->>'doc_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;


CREATE OR REPLACE FUNCTION terms_two_level(index regclass, first_field text, second_field text, query zdbquery, order_by terms_order DEFAULT 'count', size bigint DEFAULT 0) RETURNS TABLE (first_term text, second_term text, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_terms_two_level(index, first_field, second_field, query, order_by::text, size)::jsonb;
BEGIN
    RETURN QUERY
        SELECT entry->>'key',
               jsonb_array_elements(entry->'sub_agg'->'buckets')->>'key',
               (jsonb_array_elements(entry->'sub_agg'->'buckets')->>'doc_count')::bigint
        FROM jsonb_array_elements(zdb.extract_the_agg_data(index, first_field, response)->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION avg(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_avg(index, field, query)::jsonb;
BEGIN
    RETURN (zdb.extract_the_agg_data(index, field, response)->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION min(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_min(index, field, query)::jsonb;
BEGIN
    RETURN (zdb.extract_the_agg_data(index, field, response)->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION max(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_max(index, field, query)::jsonb;
BEGIN
    RETURN (zdb.extract_the_agg_data(index, field, response)->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION cardinality(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_cardinality(index, field, query)::jsonb;
BEGIN
    RETURN (zdb.extract_the_agg_data(index, field, response)->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION sum(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_sum(index, field, query)::jsonb;
BEGIN
    RETURN (zdb.extract_the_agg_data(index, field, response)->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION value_count(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_value_count(index, field, query)::jsonb;
BEGIN
    RETURN (zdb.extract_the_agg_data(index, field, response)->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION percentiles(index regclass, field text, query zdbquery, percents text DEFAULT '') RETURNS TABLE (percentile numeric, value numeric) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_percentiles(index, field, query, percents)::jsonb;
BEGIN
    RETURN QUERY select key::numeric, jsonb_object_field_text(json, key)::numeric
                 from (select key, zdb.extract_the_agg_data(index, field, response)->'values' as json
                       from jsonb_object_keys(zdb.extract_the_agg_data(index, field, response)->'values') key
                      ) x;
END;
$$;

CREATE OR REPLACE FUNCTION percentile_ranks(index regclass, field text, query zdbquery, "values" text DEFAULT '') RETURNS TABLE (percentile numeric, value numeric) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_percentile_ranks(index, field, query, "values")::jsonb;
BEGIN
    RETURN QUERY select key::numeric, jsonb_object_field_text(json, key)::numeric
                 from (select key, zdb.extract_the_agg_data(index, field, response)->'values' as json
                       from jsonb_object_keys(zdb.extract_the_agg_data(index, field, response)->'values') key
                      ) x;
END;
$$;

CREATE OR REPLACE FUNCTION stats(index regclass, field text, query zdbquery) RETURNS TABLE (count bigint, min numeric, max numeric, avg numeric, sum numeric) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_stats(index, field, query)::jsonb;
BEGIN
    RETURN QUERY
        SELECT
            (zdb.extract_the_agg_data(index, field, response)->>'count')::bigint,
            (zdb.extract_the_agg_data(index, field, response)->>'min')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'max')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'avg')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'sum')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION extended_stats(index regclass, field text, query zdbquery, sigma int DEFAULT 0) RETURNS TABLE (count bigint, min numeric, max numeric, avg numeric, sum numeric, sum_of_squares numeric, variance numeric, stddev numeric, stddev_upper numeric, stddev_lower numeric) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_extended_stats(index, field, query, sigma)::jsonb;
BEGIN
    RETURN QUERY
        SELECT
            (zdb.extract_the_agg_data(index, field, response)->>'count')::bigint,
            (zdb.extract_the_agg_data(index, field, response)->>'min')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'max')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'avg')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'sum')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'sum_of_squares')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'variance')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->>'std_deviation')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->'std_deviation_bounds'->>'upper')::numeric,
            (zdb.extract_the_agg_data(index, field, response)->'std_deviation_bounds'->>'lower')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION zdb.significant_terms(index regclass, field text, query zdbquery, include text DEFAULT '.*', size int DEFAULT 10, min_doc_count int DEFAULT 3) RETURNS TABLE (term text, doc_count bigint, score numeric, bg_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_significant_terms(index, field, query, include, size, min_doc_count)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key', (entry->>'doc_count')::bigint, (entry->>'score')::numeric, (entry->>'bg_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION significant_terms_two_level(index regclass, first_field text, second_field text, query zdbquery, size bigint DEFAULT 0) RETURNS TABLE (first_term text, second_term text, doc_count bigint, score numeric, bg_count bigint, doc_count_error_upper_bound bigint, sum_other_doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_significant_terms_two_level(index, first_field, second_field, query, size)::jsonb;
BEGIN
    RETURN QUERY
        SELECT NULL::text, NULL::text, NULL::bigint, NULL::numeric, NULL::bigint,
               (zdb.extract_the_agg_data(index, first_field, response)->>'doc_count_error_upper_bound')::bigint,
               (zdb.extract_the_agg_data(index, first_field, response)->>'sum_other_doc_count')::bigint
        UNION ALL
        SELECT entry->>'key',
               jsonb_array_elements(entry->'sub_agg'->'buckets')->>'key',
               (jsonb_array_elements(entry->'sub_agg'->'buckets')->>'doc_count')::bigint,
               (jsonb_array_elements(entry->'sub_agg'->'buckets')->>'score')::numeric,
               (jsonb_array_elements(entry->'sub_agg'->'buckets')->>'bg_count')::bigint,
               NULL::bigint, NULL::bigint
        FROM jsonb_array_elements(zdb.extract_the_agg_data(index, first_field, response)->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION range(index regclass, field text, query zdbquery, ranges_array json) RETURNS TABLE (key text, "from" numeric, "to" numeric, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_range(index, field, query, ranges_array)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key',
                        (entry->>'from')::numeric,
                        (entry->>'to')::numeric,
                        (entry->>'doc_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION date_range(index regclass, field text, query zdbquery, date_ranges_array json) RETURNS TABLE (key text, "from" numeric, from_as_string timestamp with time zone, "to" numeric, to_as_string timestamp with time zone, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_date_range(index, field, query, date_ranges_array)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key',
                        (entry->>'from')::numeric,
                        (entry->>'from_as_string')::timestamp with time zone,
                        (entry->>'to')::numeric,
                        (entry->>'to_as_string')::timestamp with time zone,
                        (entry->>'doc_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION zdb.histogram(index regclass, field text, query zdbquery, "interval" float8, min_doc_count int DEFAULT 0) RETURNS TABLE (key numeric, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_histogram(index, field, query, "interval", min_doc_count)::jsonb;
BEGIN
    RETURN QUERY SELECT (entry->>'key')::numeric,
                        (entry->>'doc_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION date_histogram(index regclass, field text, query zdbquery, "interval" text, format text DEFAULT 'yyyy-MM-dd') RETURNS TABLE (key numeric, key_as_string text, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_date_histogram(index, field, query, "interval", format)::jsonb;
BEGIN
    RETURN QUERY SELECT (entry->>'key')::numeric,
                        entry->>'key_as_string',
                        (entry->>'doc_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION missing(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_missing(index, field, query)::jsonb;
BEGIN
    RETURN (zdb.extract_the_agg_data(index, field, response)->>'doc_count')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION ip_range(index regclass, field text, query zdbquery, ip_ranges_array json) RETURNS TABLE (key text, "from" inet, "to" inet, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_ip_range(index, field, query, ip_ranges_array)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key',
                        (entry->>'from')::inet,
                        (entry->>'to')::inet,
                        (entry->>'doc_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION significant_text(index regclass, field text, query zdbquery, sample_size int DEFAULT 0, filter_duplicate_text boolean DEFAULT true) RETURNS TABLE (term text, doc_count bigint, score numeric, bg_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_significant_text(index, field, query, sample_size, filter_duplicate_text)::jsonb;
BEGIN
    RETURN QUERY SELECT NULL::text, (zdb.extract_the_agg_data(index, field, response)->>'doc_count')::bigint, NULL::numeric, (zdb.extract_the_agg_data(index, field, response)->>'bg_count')::bigint
                 UNION ALL
                 SELECT entry->>'key', (entry->>'doc_count')::bigint, (entry->>'score')::numeric, (entry->>'bg_count')::bigint
                 FROM jsonb_array_elements(zdb.extract_the_agg_data(index, field, response)->'buckets') entry;
END;
$$;

