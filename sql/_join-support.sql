CREATE OR REPLACE FUNCTION dsl.join(left_field name, index regclass, right_field name, query zdbquery,
                                    size int DEFAULT 0) RETURNS zdbquery
    PARALLEL SAFE STABLE
    LANGUAGE plpgsql AS
$$
BEGIN
    IF size > 0 THEN
        /* if we have a size limit, then limit to the top matching hits */
        RETURN dsl.bool(dsl.filter(
                dsl.terms(left_field, VARIADIC (SELECT coalesce(array_agg(source ->> right_field), ARRAY []::text[])
                                                FROM zdb.top_hits(index, ARRAY [right_field], query, size)))));
    ELSE
        /* otherwise, return all the matching terms */
        RETURN dsl.bool(dsl.filter(dsl.terms(left_field, VARIADIC zdb.terms_array(index, right_field, query))));
    END IF;
END ;
$$;
