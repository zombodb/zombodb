CREATE OR REPLACE FUNCTION terms_array(index regclass, field text, query zdbquery, size_limit bigint DEFAULT 0, order_by terms_order DEFAULT 'count') RETURNS text[] LANGUAGE sql AS $$
    SELECT zdb.internal_terms_array(index, field, query, order_by::text, size_limit);
$$;

DROP FUNCTION join(name, regclass, name, zdbquery, int);
CREATE OR REPLACE FUNCTION dsl.join(left_field name, index regclass, right_field name, query zdbquery, size int DEFAULT 0) RETURNS zdbquery PARALLEL SAFE STABLE LANGUAGE plpgsql AS $$
BEGIN
    IF size > 0 THEN
        /* if we have a size limit, then limit to the top matching hits */
        RETURN dsl.filter(terms(left_field, VARIADIC (SELECT array_agg(source->>right_field) FROM zdb.top_hits(index, ARRAY[right_field], query, size))));
    ELSE
        /* otherwise, return all the matching terms */
        RETURN dsl.filter(terms(left_field, VARIADIC zdb.terms_array(index, right_field, query)));
    END IF;
END;
$$;
