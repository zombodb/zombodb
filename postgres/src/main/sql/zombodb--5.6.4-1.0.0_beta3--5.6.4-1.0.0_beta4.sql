CREATE OR REPLACE FUNCTION zdb_tally(table_name regclass, fieldname text, is_nested boolean, stem text, query text, max_terms bigint, sort_order zdb_tally_order, shard_size int DEFAULT 0) RETURNS SETOF zdb_tally_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
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

  json_data := zdb_internal_tally(type_oid, CASE WHEN is_nested THEN format('#nested(%s)', fieldname) ELSE fieldname END, stem, query, max_terms, sort_order::text, shard_size);
  nested := (json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets') IS NOT NULL;
  
  IF nested THEN
    missing := (json_data->'aggregations'->'nested'->'filter'->'missing'->>'doc_count')::bigint;
    buckets := json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets';
  ELSEIF (json_data->'aggregations'->'nested'->fieldname->'buckets') IS NOT NULL THEN
    missing := (json_data->'aggregations'->'nested'->'missing'->>'doc_count')::bigint;
    buckets := json_data->'aggregations'->'nested'->fieldname->'buckets';
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

CREATE OR REPLACE FUNCTION zdb_range_agg(table_name regclass, fieldname text, is_nested boolean, range_spec json, user_query text) RETURNS SETOF zdb_range_agg_response LANGUAGE plpgsql STRICT IMMUTABLE AS $$
DECLARE
  nested    boolean;
  json_data json;
  buckets   json;
BEGIN
  json_data := zdb_internal_range_agg(zdb_determine_index(table_name), CASE WHEN is_nested THEN format('#nested(%s)', fieldname) ELSE fieldname END, range_spec, user_query);
  nested := (json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets') IS NOT NULL;

  IF nested THEN
    buckets := json_data->'aggregations'->'nested'->'filter'->fieldname->'buckets';
  ELSEIF (json_data->'aggregations'->'nested'->fieldname->'buckets') IS NOT NULL THEN
    buckets := json_data->'aggregations'->'nested'->fieldname->'buckets';
  ELSE
    buckets := json_data -> 'aggregations' -> fieldname -> 'buckets';
  END IF;

  RETURN QUERY
  SELECT
    (e->>'key')::text                   as key,
    (e->>'from')::double precision      as low,
    (e->>'to')::double precision        as high,
    (e->>'doc_count')::int8             as doc_count
  FROM json_array_elements(buckets) e;
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
  ELSEIF (json_data->'aggregations'->'nested'->fieldname->'buckets') IS NOT NULL THEN
    buckets := json_data->'aggregations'->'nested'->fieldname->'buckets';
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

CREATE OR REPLACE FUNCTION zdb_extended_stats(table_name regclass, fieldname text, is_nested boolean, query text) RETURNS SETOF zdb_extended_stats_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
  json_data json;
  type_oid oid;
  missing bigint;
  nested boolean;
BEGIN
  type_oid := zdb_determine_index(table_name);
  json_data := zdb_internal_extended_stats(type_oid, CASE WHEN is_nested THEN format('#nested(%s)', fieldname) ELSE fieldname END, query);
  missing := (json_data->'aggregations'->'missing'->>'doc_count')::bigint;
  nested := (json_data->'aggregations'->'nested'->'filter'->fieldname) IS NOT NULL;

  IF nested THEN
    json_data := json_data->'aggregations'->'nested'->'filter'->fieldname;
  ELSEIF (json_data->'aggregations'->'nested'->fieldname->'buckets') IS NOT NULL THEN
    json_data := json_data->'aggregations'->'nested'->fieldname->'buckets';
  ELSE
    json_data := json_data->'aggregations'->fieldname;
  END IF;

  RETURN QUERY (
    SELECT
      (json_data->>'count')::int8,
      (json_data->>'sum')::float8,
      (json_data->>'min')::float8,
      (json_data->>'max')::float8,
      (json_data->>'avg')::float8,
      (json_data->>'sum_of_squares')::float8,
      (json_data->>'variance')::float8,
      (json_data->>'std_deviation')::float8
  );
END;
$$;