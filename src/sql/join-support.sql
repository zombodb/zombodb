--
-- simple cross-index join support... requires both dsl and agg functions already created
--
CREATE OR REPLACE FUNCTION dsl.join(left_field text, index regclass, right_field text, query zdbquery, size int DEFAULT 0) RETURNS zdbquery PARALLEL SAFE STABLE LANGUAGE plpgsql AS $$
BEGIN
    IF size > 0 THEN
        /* if we have a size limit, then limit to the top matching hits */
        RETURN dsl.bool(dsl.filter(dsl.terms(left_field, VARIADIC coalesce((SELECT array_agg(source->>right_field) FROM zdb.top_hits(index, ARRAY[right_field], query, size)), ARRAY[]::text[]))));
    ELSE
        /* otherwise, return all the matching terms */
        RETURN dsl.bool(dsl.filter(dsl.terms(left_field, VARIADIC coalesce(zdb.terms_array(index, right_field, query), ARRAY[]::text[]))));
    END IF;
END;
$$;

