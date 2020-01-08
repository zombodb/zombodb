CREATE OR REPLACE FUNCTION dsl.terms_array(field name, "values" anyarray) RETURNS zdbquery PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(json_build_object('terms', json_build_object(field, "values")))::zdbquery;
$$;
