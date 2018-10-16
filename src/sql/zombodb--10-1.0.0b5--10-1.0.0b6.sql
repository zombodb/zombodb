--
-- no longer necessary
--
DROP FUNCTION zdbquery(count_estimation integer, user_query text);
DROP FUNCTION zdbquery(count_estimation integer, user_query json);


--
-- changing the return type of these two functions
--
DROP FUNCTION to_query_dsl(zdbquery);
DROP FUNCTION to_queries_dsl(queries zdbquery[]);
CREATE OR REPLACE FUNCTION to_query_dsl(zdbquery) RETURNS json PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_to_query_dsl';
CREATE OR REPLACE FUNCTION to_queries_dsl(queries zdbquery[]) RETURNS json[] PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT array_agg(zdb.to_query_dsl(query)) FROM unnest(queries) query;
$$;


--
-- functions to change query properties
--
CREATE OR REPLACE FUNCTION zdb.set_query_property(property text, value text, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_set_query_property';

CREATE OR REPLACE FUNCTION dsl.offset_limit("offset" bigint, "limit" bigint, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT zdb.set_query_property('limit', "limit"::text, zdb.set_query_property('offset', "offset"::text, query));
$$;

CREATE OR REPLACE FUNCTION dsl.limit("limit" bigint, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT zdb.set_query_property('limit', "limit"::text, query);
$$;

CREATE OR REPLACE FUNCTION dsl.offset("offset" bigint, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT zdb.set_query_property('offset', "offset"::text, query);
$$;

CREATE TYPE dsl.es_sort_directions AS ENUM ('asc', 'desc');
CREATE OR REPLACE FUNCTION dsl.sort(sort_field text, sort_direction dsl.es_sort_directions, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT zdb.set_query_property('sort_direction', sort_direction::text, zdb.set_query_property('sort_field', sort_field::text, query));
$$;

CREATE OR REPLACE FUNCTION dsl.maxscore(maxscore real, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT zdb.set_query_property('maxscore', maxscore::text, query);
$$;

CREATE OR REPLACE FUNCTION dsl.row_estimate(row_estimate real, query zdbquery) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT zdb.set_query_property('row_estimate', row_estimate::text, query);
$$;
