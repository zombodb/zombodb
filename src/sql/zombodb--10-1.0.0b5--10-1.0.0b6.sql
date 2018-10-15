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
