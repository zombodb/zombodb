DROP FUNCTION dsl.bool;
DROP FUNCTION dsl.must;
DROP FUNCTION dsl.must_not;
DROP FUNCTION dsl.should;
DROP FUNCTION dsl.filter;

DROP TYPE dsl.esqdsl_bool;
DROP TYPE dsl.esqdsl_must;
DROP TYPE dsl.esqdsl_must_not;
DROP TYPE dsl.esqdsl_should;
DROP TYPE dsl.esqdsl_filter;

CREATE TYPE dsl.esqdsl_bool_part AS (query zdbquery);

CREATE OR REPLACE FUNCTION dsl.bool(VARIADIC queries dsl.esqdsl_bool_part[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_build_object('bool',
        (
            SELECT json_object_agg(type, query_array) FROM (
                SELECT type, json_agg(query) query_array FROM (
                    WITH queries AS (SELECT (unnest(queries)).query::json AS query)
                    SELECT 'should' AS type, json_array_elements(queries.query->'bool'->'should') AS query FROM queries WHERE queries.query->'bool'->'should' IS NOT NULL
                       UNION ALL
                    SELECT 'must', json_array_elements(queries.query->'bool'->'must') FROM queries WHERE queries.query->'bool'->'must' IS NOT NULL
                       UNION ALL
                    SELECT 'must_not', json_array_elements(queries.query->'bool'->'must_not') FROM queries WHERE queries.query->'bool'->'must_not' IS NOT NULL
                       UNION ALL
                    SELECT 'filter', json_array_elements(queries.query->'bool'->'filter') FROM queries WHERE queries.query->'bool'->'filter' IS NOT NULL
                ) x GROUP BY type ORDER BY type
            ) x
        )
    )::zdbquery;
$$;

CREATE OR REPLACE FUNCTION dsl.should(VARIADIC queries zdbquery[]) RETURNS dsl.esqdsl_bool_part PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ROW(json_build_object('bool', json_build_object('should', queries)))::dsl.esqdsl_bool_part;
$$;
CREATE OR REPLACE FUNCTION dsl.must(VARIADIC queries zdbquery[]) RETURNS dsl.esqdsl_bool_part PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ROW(json_build_object('bool', json_build_object('must', queries)))::dsl.esqdsl_bool_part;
$$;
CREATE OR REPLACE FUNCTION dsl.must_not(VARIADIC queries zdbquery[]) RETURNS dsl.esqdsl_bool_part PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ROW(json_build_object('bool', json_build_object('must_not', queries)))::dsl.esqdsl_bool_part;
$$;
CREATE OR REPLACE FUNCTION dsl.filter(VARIADIC queries zdbquery[]) RETURNS dsl.esqdsl_bool_part PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT ROW(json_build_object('bool', json_build_object('filter', queries)))::dsl.esqdsl_bool_part;
$$;
CREATE OR REPLACE FUNCTION dsl.noteq(query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.bool(dsl.must_not(query));
$$;
CREATE OR REPLACE FUNCTION dsl.not(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.bool(dsl.must_not(VARIADIC queries));
$$;
CREATE OR REPLACE FUNCTION dsl.and(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.bool(dsl.must(VARIADIC queries));
$$;
CREATE OR REPLACE FUNCTION dsl.or(VARIADIC queries zdbquery[]) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT dsl.bool(dsl.should(VARIADIC queries));
$$;
