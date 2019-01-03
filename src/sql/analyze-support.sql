--
-- analyzer api support
--
CREATE OR REPLACE FUNCTION analyze_text(index regclass, analyzer text, text text) RETURNS TABLE (type text, token text, "position" int, start_offset int, end_offset int) PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT tokens->>'type',
           tokens->>'token',
           (tokens->>'position')::int,
           (tokens->>'start_offset')::int,
           (tokens->>'end_offset')::int
      FROM jsonb_array_elements((zdb.request(index, '/_analyze', 'GET', json_build_object('analyzer', analyzer, 'text', text)::text)::jsonb)->'tokens') tokens;
$$;
CREATE OR REPLACE FUNCTION analyze_custom(index regclass, text text DEFAULT NULL, tokenizer text DEFAULT NULL, normalizer text DEFAULT NULL, filter text[] DEFAULT NULL, char_filter text[] DEFAULT NULL) RETURNS TABLE (type text, token text, "position" int, start_offset int, end_offset int) PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT tokens->>'type',
           tokens->>'token',
           (tokens->>'position')::int,
           (tokens->>'start_offset')::int,
           (tokens->>'end_offset')::int
      FROM jsonb_array_elements((zdb.request(index, '_analyze', 'GET', json_strip_nulls(json_build_object('tokenizer', tokenizer, 'normalizer', normalizer, 'text', text, 'filter', filter, 'char_filter', char_filter))::text)::jsonb)->'tokens') tokens;
$$;
CREATE OR REPLACE FUNCTION analyze_with_field(index regclass, field text, text text) RETURNS TABLE (type text, token text, "position" int, start_offset int, end_offset int) PARALLEL SAFE IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT tokens->>'type',
           tokens->>'token',
           (tokens->>'position')::int,
           (tokens->>'start_offset')::int,
           (tokens->>'end_offset')::int
      FROM jsonb_array_elements((zdb.request(index, '_analyze', 'GET', json_build_object('field', field, 'text', text)::text)::jsonb)->'tokens') tokens;
$$;

