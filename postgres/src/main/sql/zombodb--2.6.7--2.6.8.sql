CREATE OR REPLACE FUNCTION zdb_tally(table_name regclass, fieldname text, is_nested boolean, stem text, query text, max_terms bigint, sort_order zdb_tally_order) RETURNS SETOF zdb_tally_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
  json_data json;
  type_oid oid;
  missing bigint;
  nested boolean;
  data_type text;
  buckets json;
BEGIN
  type_oid := zdb_determine_index(table_name);

  SELECT typname FROM pg_type WHERE oid = (SELECT atttypid FROM pg_attribute WHERE attrelid = table_name AND attname = fieldname) INTO data_type;

  json_data := zdb_internal_tally(type_oid, CASE WHEN is_nested THEN format('#nested(%s)', fieldname) ELSE fieldname END, stem, query, max_terms, sort_order::text);
  nested := (json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets') IS NOT NULL;

  IF nested THEN
    missing := (json_data->'aggregations'->'nested'->'filter'->'missing'->>'doc_count')::bigint;
    buckets := json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets';
  ELSE
    missing := (json_data->'aggregations'->'missing'->>'doc_count')::bigint;
    buckets := json_data->'aggregations'->fieldname->'buckets';
  END IF;

  IF missing IS NULL OR missing = 0 THEN
    RETURN QUERY (
      SELECT
        coalesce((x->>'key_as_string')::text, (x->>'key')::text),
        (x->>'doc_count')::int8
      FROM json_array_elements(buckets) x
    );
  ELSE
    RETURN QUERY (
      SELECT * FROM (SELECT NULL::text, missing LIMIT missing) x
      UNION ALL
      SELECT
        coalesce((x->>'key_as_string')::text, (x->>'key')::text),
        (x->>'doc_count')::int8
      FROM json_array_elements(buckets) x
    );
  END IF;
END;
$$;

CREATE OR REPLACE FUNCTION zdb_significant_terms(table_name regclass, fieldname text, is_nested boolean, stem text, query text, max_terms bigint) RETURNS SETOF zdb_significant_terms_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
  json_data json;
  type_oid oid;
  nested boolean;
  buckets json;
BEGIN
  type_oid := zdb_determine_index(table_name);
  json_data := zdb_internal_significant_terms(type_oid, CASE WHEN is_nested THEN format('#nested(%s)', fieldname) ELSE fieldname END, stem, query, max_terms);
  nested := (json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets') IS NOT NULL;

  IF nested THEN
    buckets := json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets';
  ELSE
    buckets := json_data->'aggregations'->fieldname->'buckets';
  END IF;

  RETURN QUERY (
    SELECT
      (x->>'key')::text,
      (x->>'doc_count')::int8,
      (x->>'score')::float8
    FROM json_array_elements(buckets) x
  );
END;
$$;

CREATE OR REPLACE FUNCTION zdb_suggest_terms(table_name regclass, fieldname text, base text, query text, max_terms int8) RETURNS SETOF zdb_suggest_terms_response LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
  type_oid oid;
  data json;
BEGIN
  type_oid := zdb_determine_index(table_name);
  data := zdb_internal_suggest_terms(type_oid, fieldname, base, query, max_terms);

  RETURN QUERY SELECT base, zdb_estimate_count(table_name, fieldname || ':("' || coalesce(case when trim(base) = '' then null else trim(base) end, 'null') || '") AND (' || coalesce(query, '') || ')')
               UNION ALL
               SELECT (x->>'text')::text,
                 (x->>'freq')::int8
               FROM json_array_elements(json_array_element(data->'suggest'->'suggestions', 0)->'options') x;
END;
$$;
