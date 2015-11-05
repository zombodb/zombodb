CREATE OR REPLACE FUNCTION zdb_determine_index(table_name regclass) RETURNS oid STRICT VOLATILE LANGUAGE plpgsql AS $$
DECLARE
  exp json;
  kind char;
  index_oid oid;
BEGIN
    SELECT relkind INTO kind FROM pg_class WHERE oid = table_name::oid;

    IF kind = 'r' THEN
      EXECUTE format('SET enable_seqscan TO OFF; EXPLAIN (FORMAT JSON) SELECT 1 FROM %s x WHERE zdb(''%s'', ctid) ==> '''' ', table_name, table_name) INTO exp;
    ELSE
      EXECUTE format('SET enable_seqscan TO OFF; EXPLAIN (FORMAT JSON) SELECT 1 FROM %s WHERE zdb ==> '''' ', table_name) INTO exp;
    END IF;

    IF (json_array_element(exp, 0)->'Plan'->'Plans')::text IS NOT NULL THEN
      RETURN oid FROM pg_class WHERE relname IN (SELECT unnest(regexp_matches(exp::text, '"Index Name":\s*"(.*)",*$', 'gn')))
                                 AND relam = (SELECT oid FROM pg_am WHERE amname = 'zombodb');
    END IF;

    index_oid := (json_array_element(exp, 0)->'Plan'->>'Index Name')::regclass::oid;
    IF index_oid IS NULL THEN
      RAISE EXCEPTION 'Unable to determine the index to use for %', table_name;
    END IF;
    RETURN index_oid;
END;
$$;

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
  IF stem <> '^.*' AND data_type IN ('text', '_text', 'phrase', 'phrase_array', 'fulltext', 'varchar', '_varchar') THEN
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
CREATE OR REPLACE FUNCTION zdb_tally(table_name regclass, fieldname text, stem text, query text, max_terms bigint, sort_order zdb_tally_order) RETURNS SETOF zdb_tally_response STRICT IMMUTABLE LANGUAGE sql AS $$
  SELECT zdb_tally($1, $2, false, $3, $4, $5, $6);
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
CREATE OR REPLACE FUNCTION zdb_range_agg(table_name regclass, fieldname text, range_spec json, user_query text) RETURNS SETOF zdb_range_agg_response LANGUAGE sql STRICT IMMUTABLE AS $$
  SELECT zdb_range_agg($1, $2, false, $3, $4);
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
      upper((x->>'key')::text),
      (x->>'doc_count')::int8,
      (x->>'score')::float8
    FROM json_array_elements(buckets) x
  );
END;
$$;
CREATE OR REPLACE FUNCTION zdb_significant_terms(table_name regclass, fieldname text, stem text, query text, max_terms bigint) RETURNS SETOF zdb_significant_terms_response STRICT IMMUTABLE LANGUAGE sql AS $$
  SELECT zdb_significant_terms($1, $2, false, $3, $4, $5);
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
CREATE OR REPLACE FUNCTION zdb_extended_stats(table_name regclass, fieldname text, query text) RETURNS SETOF zdb_extended_stats_response STRICT IMMUTABLE LANGUAGE sql AS $$
  SELECT zdb_extended_stats($1, $2, false, $3);
$$;
