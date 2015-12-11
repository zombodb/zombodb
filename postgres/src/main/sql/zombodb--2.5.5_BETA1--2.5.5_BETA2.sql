CREATE OR REPLACE FUNCTION zdb_tally(table_name regclass, fieldname text, is_nested boolean, stem text, query text, max_terms bigint, sort_order zdb_tally_order) RETURNS SETOF zdb_tally_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
  json_data json;
  type_oid oid;
  missing bigint;
  nested boolean;
  new_query text;
  data_type text;
  buckets json;
BEGIN
  type_oid := zdb_determine_index(table_name);

  SELECT typname FROM pg_type WHERE oid = (SELECT atttypid FROM pg_attribute WHERE attrelid = table_name AND attname = fieldname) INTO data_type;
  IF stem <> '^.*' AND strpos(stem, '\') = 0 AND data_type IN ('text', '_text', 'phrase', 'phrase_array', 'fulltext', 'varchar', '_varchar') THEN
    new_query := format('(%s) AND (%s:~"%s")', query, fieldname, split_part(stem, '^', 2));
  ELSE
    new_query := query;
  END IF;

  json_data := zdb_internal_tally(type_oid, CASE WHEN is_nested THEN format('#nested(%s)', fieldname) ELSE fieldname END, stem, new_query, max_terms, sort_order::text);
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
        coalesce(upper((x->>'key_as_string')::text), upper((x->>'key')::text)),
        (x->>'doc_count')::int8
      FROM json_array_elements(buckets) x
    );
  ELSE
    RETURN QUERY (
      SELECT * FROM (SELECT NULL::text, missing LIMIT missing) x
      UNION ALL
      SELECT
        coalesce(upper((x->>'key_as_string')::text), upper((x->>'key')::text)),
        (x->>'doc_count')::int8
      FROM json_array_elements(buckets) x
    );
  END IF;
END;
$$;