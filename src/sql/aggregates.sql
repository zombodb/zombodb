--
-- aggregate support functions
--
CREATE OR REPLACE FUNCTION count(index regclass, query zdbquery) RETURNS bigint PARALLEL SAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_count';
CREATE OR REPLACE FUNCTION raw_count(index regclass, query zdbquery) RETURNS bigint SET zdb.ignore_visibility = true PARALLEL SAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_count';
CREATE OR REPLACE FUNCTION arbitrary_agg(index regclass, query zdbquery, agg_json json) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_arbitrary_agg';

CREATE TYPE terms_order AS ENUM ('count', 'term', 'reverse_count', 'reverse_term');
CREATE OR REPLACE FUNCTION internal_terms(index regclass, field text, query zdbquery, order_by text, size_limit bigint) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_terms';
CREATE OR REPLACE FUNCTION terms(index regclass, field text, query zdbquery, size_limit bigint DEFAULT 0, order_by terms_order DEFAULT 'count') RETURNS TABLE (term text, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_terms(index, field, query, order_by::text, size_limit)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key', (entry->>'doc_count')::bigint FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;
CREATE OR REPLACE FUNCTION internal_terms_array(index regclass, field text, query zdbquery, order_by text, size_limit bigint) RETURNS text[] STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_terms_array';
CREATE OR REPLACE FUNCTION terms_array(index regclass, field text, query zdbquery, size_limit bigint DEFAULT 0, order_by terms_order DEFAULT 'count') RETURNS text[] LANGUAGE sql AS $$
    SELECT zdb.internal_terms_array(index, field, query, order_by::text, size_limit);
$$;

CREATE OR REPLACE FUNCTION internal_terms_two_level(index regclass, first_field text, second_field text, query zdbquery, order_by text, size bigint) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_terms_two_level';
CREATE OR REPLACE FUNCTION terms_two_level(index regclass, first_field text, second_field text, query zdbquery, order_by terms_order DEFAULT 'count', size bigint DEFAULT 0) RETURNS TABLE (first_term text, second_term text, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_terms_two_level(index, first_field, second_field, query, order_by::text, size)::jsonb;
BEGIN
    RETURN QUERY
                SELECT entry->>'key',
                       jsonb_array_elements(entry->'sub_agg'->'buckets')->>'key',
                       (jsonb_array_elements(entry->'sub_agg'->'buckets')->>'doc_count')::bigint
                  FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_avg(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_avg';
CREATE OR REPLACE FUNCTION avg(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_avg(index, field, query)::jsonb;
BEGIN
    RETURN (response->'aggregations'->'the_agg'->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_min(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_min';
CREATE OR REPLACE FUNCTION min(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_min(index, field, query)::jsonb;
BEGIN
    RETURN (response->'aggregations'->'the_agg'->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_max(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_max';
CREATE OR REPLACE FUNCTION max(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_max(index, field, query)::jsonb;
BEGIN
    RETURN (response->'aggregations'->'the_agg'->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_cardinality(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_cardinality';
CREATE OR REPLACE FUNCTION cardinality(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_cardinality(index, field, query)::jsonb;
BEGIN
    RETURN (response->'aggregations'->'the_agg'->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_sum(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_sum';
CREATE OR REPLACE FUNCTION sum(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_sum(index, field, query)::jsonb;
BEGIN
    RETURN (response->'aggregations'->'the_agg'->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_value_count(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_value_count';
CREATE OR REPLACE FUNCTION value_count(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_value_count(index, field, query)::jsonb;
BEGIN
    RETURN (response->'aggregations'->'the_agg'->>'value')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_percentiles(index regclass, field text, query zdbquery, percents text) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_percentiles';
CREATE OR REPLACE FUNCTION percentiles(index regclass, field text, query zdbquery, percents text DEFAULT '') RETURNS TABLE (percentile numeric, value numeric) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_percentiles(index, field, query, percents)::jsonb;
BEGIN
    RETURN QUERY select key::numeric, jsonb_object_field_text(json, key)::numeric
                   from (select key, response->'aggregations'->'the_agg'->'values' as json
                           from jsonb_object_keys(response->'aggregations'->'the_agg'->'values') key
                        ) x;
END;
$$;

CREATE OR REPLACE FUNCTION internal_percentile_ranks(index regclass, field text, query zdbquery, "values" text) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_percentile_ranks';
CREATE OR REPLACE FUNCTION percentile_ranks(index regclass, field text, query zdbquery, "values" text DEFAULT '') RETURNS TABLE (percentile numeric, value numeric) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_percentile_ranks(index, field, query, "values")::jsonb;
BEGIN
    RETURN QUERY select key::numeric, jsonb_object_field_text(json, key)::numeric
                   from (select key, response->'aggregations'->'the_agg'->'values' as json
                           from jsonb_object_keys(response->'aggregations'->'the_agg'->'values') key
                        ) x;
END;
$$;

CREATE OR REPLACE FUNCTION internal_stats(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_stats';
CREATE OR REPLACE FUNCTION stats(index regclass, field text, query zdbquery) RETURNS TABLE (count bigint, min numeric, max numeric, avg numeric, sum numeric) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_stats(index, field, query)::jsonb;
BEGIN
    RETURN QUERY
        SELECT
            (response->'aggregations'->'the_agg'->>'count')::bigint,
            (response->'aggregations'->'the_agg'->>'min')::numeric,
            (response->'aggregations'->'the_agg'->>'max')::numeric,
            (response->'aggregations'->'the_agg'->>'avg')::numeric,
            (response->'aggregations'->'the_agg'->>'sum')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_extended_stats(index regclass, field text, query zdbquery, sigma int) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_extended_stats';
CREATE OR REPLACE FUNCTION extended_stats(index regclass, field text, query zdbquery, sigma int DEFAULT 0) RETURNS TABLE (count bigint, min numeric, max numeric, avg numeric, sum numeric, sum_of_squares numeric, variance numeric, stddev numeric, stddev_upper numeric, stddev_lower numeric) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_extended_stats(index, field, query, sigma)::jsonb;
BEGIN
    RETURN QUERY
        SELECT
            (response->'aggregations'->'the_agg'->>'count')::bigint,
            (response->'aggregations'->'the_agg'->>'min')::numeric,
            (response->'aggregations'->'the_agg'->>'max')::numeric,
            (response->'aggregations'->'the_agg'->>'avg')::numeric,
            (response->'aggregations'->'the_agg'->>'sum')::numeric,
            (response->'aggregations'->'the_agg'->>'sum_of_squares')::numeric,
            (response->'aggregations'->'the_agg'->>'variance')::numeric,
            (response->'aggregations'->'the_agg'->>'std_deviation')::numeric,
            (response->'aggregations'->'the_agg'->'std_deviation_bounds'->>'upper')::numeric,
            (response->'aggregations'->'the_agg'->'std_deviation_bounds'->>'lower')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_significant_terms(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_significant_terms';
CREATE OR REPLACE FUNCTION significant_terms(index regclass, field text, query zdbquery) RETURNS TABLE (term text, doc_count bigint, score numeric, bg_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_significant_terms(index, field, query)::jsonb;
BEGIN
    RETURN QUERY SELECT NULL::text, (response->'aggregations'->'the_agg'->>'doc_count')::bigint, NULL::numeric, (response->'aggregations'->'the_agg'->>'bg_count')::bigint
                        UNION ALL
                SELECT entry->>'key', (entry->>'doc_count')::bigint, (entry->>'score')::numeric, (entry->>'bg_count')::bigint
                  FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_significant_terms_two_level(index regclass, first_field text, second_field text, query zdbquery, size bigint) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_significant_terms_two_level';
CREATE OR REPLACE FUNCTION significant_terms_two_level(index regclass, first_field text, second_field text, query zdbquery, size bigint DEFAULT 0) RETURNS TABLE (first_term text, second_term text, doc_count bigint, score numeric, bg_count bigint, doc_count_error_upper_bound bigint, sum_other_doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_significant_terms_two_level(index, first_field, second_field, query, size)::jsonb;
BEGIN
    RETURN QUERY
                SELECT NULL::text, NULL::text, NULL::bigint, NULL::numeric, NULL::bigint,
                       (response->'aggregations'->'the_agg'->>'doc_count_error_upper_bound')::bigint,
                       (response->'aggregations'->'the_agg'->>'sum_other_doc_count')::bigint
                          UNION ALL
                SELECT entry->>'key',
                       jsonb_array_elements(entry->'sub_agg'->'buckets')->>'key',
                       (jsonb_array_elements(entry->'sub_agg'->'buckets')->>'doc_count')::bigint,
                       (jsonb_array_elements(entry->'sub_agg'->'buckets')->>'score')::numeric,
                       (jsonb_array_elements(entry->'sub_agg'->'buckets')->>'bg_count')::bigint,
                       NULL::bigint, NULL::bigint
                  FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_range(index regclass, field text, query zdbquery, ranges_array json) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_range';
CREATE OR REPLACE FUNCTION range(index regclass, field text, query zdbquery, ranges_array json) RETURNS TABLE (key text, "from" numeric, "to" numeric, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_range(index, field, query, ranges_array)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key',
                        (entry->>'from')::numeric,
                        (entry->>'to')::numeric,
                        (entry->>'doc_count')::bigint
                   FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_date_range(index regclass, field text, query zdbquery, date_ranges_array json) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_date_range';
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
                   FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_histogram(index regclass, field text, query zdbquery, "interval" float8) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_histogram';
CREATE OR REPLACE FUNCTION histogram(index regclass, field text, query zdbquery, "interval" float8) RETURNS TABLE (key numeric, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_histogram(index, field, query, "interval")::jsonb;
BEGIN
    RETURN QUERY SELECT (entry->>'key')::numeric,
                        (entry->>'doc_count')::bigint
                   FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_date_histogram(index regclass, field text, query zdbquery, "interval" text, format text) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_date_histogram';
CREATE OR REPLACE FUNCTION date_histogram(index regclass, field text, query zdbquery, "interval" text, format text DEFAULT 'yyyy-MM-dd') RETURNS TABLE (key numeric, key_as_string text, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_date_histogram(index, field, query, "interval", format)::jsonb;
BEGIN
    RETURN QUERY SELECT (entry->>'key')::numeric,
                        entry->>'key_as_string',
                        (entry->>'doc_count')::bigint
                   FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_missing(index regclass, field text, query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_missing';
CREATE OR REPLACE FUNCTION missing(index regclass, field text, query zdbquery) RETURNS numeric LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_missing(index, field, query)::jsonb;
BEGIN
    RETURN (response->'aggregations'->'the_agg'->>'doc_count')::numeric;
END;
$$;

CREATE OR REPLACE FUNCTION internal_filters(index regclass, labels text[], filters zdbquery[]) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_filters';
CREATE OR REPLACE FUNCTION filters(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE (label text, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_filters(index, labels, filters)::jsonb;
BEGIN
    RETURN QUERY SELECT entry::text,
                        (response->'aggregations'->'the_agg'->'buckets'->entry->>'doc_count')::bigint
                   FROM jsonb_object_keys(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_ip_range(index regclass, field text, query zdbquery, ip_ranges_array json) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_ip_range';
CREATE OR REPLACE FUNCTION ip_range(index regclass, field text, query zdbquery, ip_ranges_array json) RETURNS TABLE (key text, "from" inet, "to" inet, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_ip_range(index, field, query, ip_ranges_array)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key',
                        (entry->>'from')::inet,
                        (entry->>'to')::inet,
                        (entry->>'doc_count')::bigint
                   FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_significant_text(index regclass, field text, query zdbquery, sample_size int, filter_duplicate_text boolean) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_significant_text';
CREATE OR REPLACE FUNCTION significant_text(index regclass, field text, query zdbquery, sample_size int DEFAULT 0, filter_duplicate_text boolean DEFAULT true) RETURNS TABLE (term text, doc_count bigint, score numeric, bg_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_significant_text(index, field, query, sample_size, filter_duplicate_text)::jsonb;
BEGIN
    RETURN QUERY SELECT NULL::text, (response->'aggregations'->'the_agg'->>'doc_count')::bigint, NULL::numeric, (response->'aggregations'->'the_agg'->>'bg_count')::bigint
                        UNION ALL
                SELECT entry->>'key', (entry->>'doc_count')::bigint, (entry->>'score')::numeric, (entry->>'bg_count')::bigint
                  FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION internal_adjacency_matrix(index regclass, labels text[], filters zdbquery[]) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_adjacency_matrix';
CREATE OR REPLACE FUNCTION adjacency_matrix(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE (key text, doc_count bigint) LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_adjacency_matrix(index, labels, filters)::jsonb;
BEGIN
    RETURN QUERY SELECT entry->>'key',
                        (entry->>'doc_count')::bigint
                   FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'buckets') entry;
END;
$$;

CREATE OR REPLACE FUNCTION adjacency_matrix_2x2(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT key, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE key = labels[1]),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE key = labels[2])

$$;

CREATE OR REPLACE FUNCTION adjacency_matrix_3x3(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT key, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE key = labels[1]),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE key = labels[2]),
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE key = labels[3])

$$;

CREATE OR REPLACE FUNCTION adjacency_matrix_4x4(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text, "4" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT key, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3], labels[4]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE key = labels[1]),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[4], labels[4]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE key = labels[2]),
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[4], labels[4]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE key = labels[3]),
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[4], labels[4]||'&'||labels[3]))
   UNION ALL
SELECT labels[4],
    (SELECT doc_count FROM matrix WHERE key in (labels[4]||'&'||labels[1], labels[1]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE key in (labels[4]||'&'||labels[2], labels[2]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE key in (labels[4]||'&'||labels[3], labels[3]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE key = labels[4])

$$;

CREATE OR REPLACE FUNCTION adjacency_matrix_5x5(index regclass, labels text[], filters zdbquery[]) RETURNS TABLE ("-" text, "1" text, "2" text, "3" text, "4" text, "5" text) STABLE LANGUAGE sql AS $$

WITH matrix AS (SELECT key, doc_count::text FROM zdb.adjacency_matrix(index, labels, filters))
SELECT NULL::text, labels[1], labels[2], labels[3], labels[4], labels[5]
   UNION ALL
SELECT labels[1],
    (SELECT doc_count FROM matrix WHERE key = labels[1]),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[2], labels[2]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[3], labels[3]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[4], labels[4]||'&'||labels[1])),
    (SELECT doc_count FROM matrix WHERE key in (labels[1]||'&'||labels[5], labels[5]||'&'||labels[1]))
   UNION ALL
SELECT labels[2],
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[1], labels[1]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE key = labels[2]),
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[3], labels[3]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[4], labels[4]||'&'||labels[2])),
    (SELECT doc_count FROM matrix WHERE key in (labels[2]||'&'||labels[5], labels[5]||'&'||labels[2]))
   UNION ALL
SELECT labels[3],
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[1], labels[1]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[2], labels[2]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE key = labels[3]),
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[4], labels[4]||'&'||labels[3])),
    (SELECT doc_count FROM matrix WHERE key in (labels[3]||'&'||labels[5], labels[5]||'&'||labels[3]))
   UNION ALL
SELECT labels[4],
    (SELECT doc_count FROM matrix WHERE key in (labels[4]||'&'||labels[1], labels[1]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE key in (labels[4]||'&'||labels[2], labels[2]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE key in (labels[4]||'&'||labels[3], labels[3]||'&'||labels[4])),
    (SELECT doc_count FROM matrix WHERE key = labels[4]),
    (SELECT doc_count FROM matrix WHERE key in (labels[4]||'&'||labels[5], labels[5]||'&'||labels[4]))
   UNION ALL
SELECT labels[5],
    (SELECT doc_count FROM matrix WHERE key in (labels[5]||'&'||labels[1], labels[1]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE key in (labels[5]||'&'||labels[2], labels[2]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE key in (labels[5]||'&'||labels[3], labels[3]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE key in (labels[5]||'&'||labels[4], labels[4]||'&'||labels[5])),
    (SELECT doc_count FROM matrix WHERE key = labels[5])

$$;

CREATE OR REPLACE FUNCTION internal_matrix_stats(index regclass, fields text[], query zdbquery) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_matrix_stats';
CREATE OR REPLACE FUNCTION matrix_stats(index regclass, fields text[], query zdbquery) RETURNS TABLE (name text, count bigint, mean numeric, variance numeric, skewness numeric, kurtosis numeric, covariance json, correlation json) STABLE LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_matrix_stats(index, fields, query)::jsonb;
BEGIN
    RETURN QUERY
        SELECT
            field->>'name',
            (field->>'count')::bigint,
            (field->>'mean')::numeric,
            (field->>'variance')::numeric,
            (field->>'skewness')::numeric,
            (field->>'kurtosis')::numeric,
            jsonb_pretty(field->'covariance')::json,
            jsonb_pretty(field->'correlation')::json
          FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'fields') field;
END;
$$;

CREATE OR REPLACE FUNCTION internal_top_hits(index regclass, fields text[], query zdbquery, size int) RETURNS json STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_internal_top_hits';
CREATE OR REPLACE FUNCTION top_hits(index regclass, fields text[], query zdbquery, size int) RETURNS TABLE (ctid tid, score float4, source json) STABLE LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_top_hits(index, fields, query, size)::jsonb;
BEGIN
    RETURN QUERY
        SELECT zdb.ctid((hit->>'_id')::bigint),
               (hit->>'_score')::float4,
               jsonb_pretty(hit->'_source')::json
          FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'hits'->'hits') hit;
END;
$$;

CREATE OR REPLACE FUNCTION top_hits_with_id(index regclass, fields text[], query zdbquery, size int) RETURNS TABLE (_id text, score float4, source json) STABLE LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_top_hits(index, fields, query, size)::jsonb;
BEGIN
    RETURN QUERY
        SELECT hit->>'_id',
               (hit->>'_score')::float4,
               jsonb_pretty(hit->'_source')::json
          FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'hits'->'hits') hit;
END;
$$;
