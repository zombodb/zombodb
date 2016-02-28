CREATE DOMAIN phrase AS text;
CREATE DOMAIN phrase_array AS text[];
CREATE DOMAIN fulltext AS text;

CREATE OR REPLACE FUNCTION sort_helper(value anyelement) RETURNS anyelement LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    _type regtype;
BEGIN
    _type := pg_typeof(value);

    IF _type IN ('date', 'timestamp', 'timestamp with time zone', 'time', 'smallint', 'integer', 'bigint', 'numeric', 'float', 'double precision', 'boolean') THEN
        -- no translation required, use as-is
        RETURN value;
    ELSEIF _type IN ('varchar', 'text', 'public.phrase', 'public.fulltext') THEN
        -- return a lower-cased, trimmed version of the value, truncated to 256 characters
        RETURN lower(trim(substring(value::text, 1, 256)));
    ELSE
        RAISE EXCEPTION 'Cannot sort data type of %', _type;
    END IF;
END;
$$;

CREATE OR REPLACE VIEW pg_locks_pretty AS
     SELECT (select nspname from pg_namespace where oid = relnamespace) as schema,
            relname,
            (SELECT usename FROM pg_stat_activity WHERE pid = pid LIMIT 1) As username,
            application_name,
            pg_locks.*,
            case when state = 'active' then query else NULL end as query,
            (now() - state_change)::interval as idle_time
       FROM pg_locks
 INNER JOIN pg_class ON pg_locks.relation = pg_class.oid
 RIGHT JOIN pg_stat_activity ON pg_locks.pid = pg_stat_activity.pid
   ORDER BY mode, relname;


--
-- simple function to convert an array to uppercase
--
CREATE OR REPLACE FUNCTION array_upper(v text[]) RETURNS text[] IMMUTABLE LANGUAGE sql AS $$ SELECT upper($1::text)::text[]; $$;

--
-- ZOMBODB SPECIFIC STUFF HERE
--

CREATE OR REPLACE FUNCTION zdbinsert(internal, internal, internal, internal, internal, internal) RETURNS boolean LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbbeginscan(internal, internal, internal) RETURNS internal LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbgettuple(internal, internal) RETURNS boolean LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbrescan(internal, internal, internal, internal, internal) RETURNS void LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbendscan(internal) RETURNS void LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbmarkpos(internal) RETURNS void LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbrestrpos(internal) RETURNS void LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbbuild(internal, internal, internal) RETURNS internal LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbbuildempty(internal) RETURNS void LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbbulkdelete(internal, internal, internal, internal) RETURNS internal LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbvacuumcleanup(internal, internal) RETURNS internal LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbcostestimate(internal, internal, internal, internal, internal, internal, internal) RETURNS void LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdbsel(internal, oid, internal, integer) RETURNS float8 LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdboptions(text[], boolean) RETURNS bytea LANGUAGE c STABLE STRICT AS '$libdir/plugins/zombodb';

--
-- convenience methods for index creation and querying
--
CREATE OR REPLACE FUNCTION zdb(record) RETURNS json LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb', 'zdb_row_to_json';
CREATE OR REPLACE FUNCTION zdb(table_name regclass, ctid tid) RETURNS tid LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb', 'zdb_table_ref_and_tid';
CREATE OR REPLACE FUNCTION zdb_num_hits() RETURNS int8 AS '$libdir/plugins/zombodb' language c;
CREATE OR REPLACE FUNCTION zdb_query_func(json, text) RETURNS bool LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb' COST 2147483647;
CREATE OR REPLACE FUNCTION zdb_tid_query_func(tid, text) RETURNS bool LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb' COST 1;


--
-- trigger support
--
CREATE OR REPLACE FUNCTION zdbtupledeletedtrigger() RETURNS trigger AS '$libdir/plugins/zombodb' language c;
CREATE OR REPLACE FUNCTION zdbeventtrigger() RETURNS event_trigger AS '$libdir/plugins/zombodb' language c;
CREATE EVENT TRIGGER zdb_alter_table_trigger ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE PROCEDURE zdbeventtrigger();


--
-- utility functions
--
CREATE OR REPLACE FUNCTION rest_get(url text) RETURNS json AS '$libdir/plugins/zombodb' language c;

--
-- index inspection functions
--
CREATE OR REPLACE FUNCTION zdb_determine_index(table_name regclass) RETURNS oid LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_get_index_name(index_name regclass) RETURNS text AS '$libdir/plugins/zombodb' language c;
CREATE OR REPLACE FUNCTION zdb_get_url(index_name regclass) RETURNS text AS '$libdir/plugins/zombodb' language c;


--
-- scoring support
--
CREATE OR REPLACE FUNCTION zdb_score_internal(table_name regclass, ctid tid) RETURNS float4 LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_score(table_name regclass, ctid tid) RETURNS float4 LANGUAGE sql IMMUTABLE STRICT AS $$
SELECT zdb_score_internal(zdb_determine_index($1), $2);
$$;



CREATE OR REPLACE FUNCTION count_of_table(table_name REGCLASS) RETURNS INT8 LANGUAGE plpgsql AS $$
DECLARE
  cnt INT8;
BEGIN
  EXECUTE format('SELECT count(*) FROM %s', table_name)
  INTO cnt;
  RETURN cnt;
END;
$$;

CREATE VIEW zdb_index_stats AS
  WITH stats AS (
      SELECT
        indrelid :: REGCLASS AS                                                                    table_name,
        zdb_get_index_name(indexrelid)                                                             index_name,
        zdb_get_url(indexrelid)                                                                    url,
        rest_get(zdb_get_url(indexrelid) || zdb_get_index_name(indexrelid) || '/_stats?pretty')    stats,
        rest_get(zdb_get_url(indexrelid) || zdb_get_index_name(indexrelid) || '/_settings?pretty') settings
      FROM pg_index
      WHERE pg_get_indexdef(indexrelid) ILIKE '%zombodb%'
  )
  SELECT
    index_name,
    url,
    table_name,
    stats -> '_all' -> 'primaries' -> 'docs' -> 'count'                                     AS es_docs,
    pg_size_pretty((stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8) AS es_size,
    (stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8                 AS es_size_bytes,
    count_of_table(table_name)                                                              AS pg_docs,
    pg_size_pretty(pg_total_relation_size(table_name))                                      AS pg_size,
    pg_total_relation_size(table_name)                                                      AS pg_size_bytes,
    stats -> '_shards' -> 'total'                                                           AS shards,
    settings -> index_name -> 'settings' -> 'index' ->> 'number_of_replicas'                AS replicas
  FROM stats;

CREATE VIEW zdb_index_stats_fast AS
  WITH stats AS (
      SELECT
        indrelid :: REGCLASS AS                                                                    table_name,
        zdb_get_index_name(indexrelid)                                                             index_name,
        zdb_get_url(indexrelid)                                                                    url,
        rest_get(zdb_get_url(indexrelid) || zdb_get_index_name(indexrelid) || '/_stats?pretty')    stats,
        rest_get(zdb_get_url(indexrelid) || zdb_get_index_name(indexrelid) || '/_settings?pretty') settings
      FROM pg_index
      WHERE pg_get_indexdef(indexrelid) ILIKE '%zombodb%'
  )
  SELECT
    index_name,
    url,
    table_name,
    stats -> '_all' -> 'primaries' -> 'docs' -> 'count'                                     AS es_docs,
    pg_size_pretty((stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8) AS es_size,
    (stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8                 AS es_size_bytes,
    (SELECT reltuples::int8 FROM pg_class WHERE oid = table_name)                           AS pg_docs_estimate,
    pg_size_pretty(pg_total_relation_size(table_name))                                      AS pg_size,
    pg_total_relation_size(table_name)                                                      AS pg_size_bytes,
    stats -> '_shards' -> 'total'                                                           AS shards,
    settings -> index_name -> 'settings' -> 'index' ->> 'number_of_replicas'                AS replicas
  FROM stats;

CREATE OR REPLACE FUNCTION zdb_internal_actual_index_record_count(type_oid oid, type_name text) RETURNS bigint STRICT IMMUTABLE LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_actual_index_record_count(table_name regclass, type_name text) RETURNS bigint STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT zdb_internal_actual_index_record_count(zdb_determine_index(table_name), type_name);
$$;

CREATE OR REPLACE FUNCTION zdb_internal_estimate_count(type_oid oid, query text) RETURNS bigint LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_estimate_count(table_name regclass, query text) RETURNS bigint STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT zdb_internal_estimate_count(zdb_determine_index(table_name), query)
$$;

CREATE OR REPLACE FUNCTION zdb_internal_describe_nested_object(type_oid oid, fieldname text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_describe_nested_object(table_name regclass, fieldname text) RETURNS json STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT zdb_internal_describe_nested_object(zdb_determine_index(table_name), fieldname);
$$;

CREATE OR REPLACE FUNCTION zdb_internal_get_index_mapping(type_oid oid) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_get_index_mapping(table_name regclass) RETURNS json STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT zdb_internal_get_index_mapping(zdb_determine_index(table_name));
$$;

CREATE TYPE zdb_get_index_field_lists_response AS (fieldname text, fields text[]);
CREATE OR REPLACE FUNCTION zdb_internal_get_index_field_lists(index_oid oid) RETURNS text LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_get_index_field_lists(table_name regclass) RETURNS SETOF zdb_get_index_field_lists_response LANGUAGE sql STRICT IMMUTABLE AS $$
    select trim((regexp_matches(x, '(.*)='))[1]) as fieldname, string_to_array((regexp_matches(x, '=\s*\[(.*?)(\]|$)'))[1], ',') fields
      from regexp_split_to_table(zdb_internal_get_index_field_lists(zdb_determine_index($1)), '\]\s*,') x;
$$;


CREATE TYPE zdb_tally_order AS ENUM ('count', 'term', 'reverse_count', 'reverse_term');
CREATE TYPE zdb_tally_response AS (term text, count bigint);
CREATE OR REPLACE FUNCTION zdb_internal_tally(type_oid oid, fieldname text, stem text, query text, max_terms bigint, sort_order text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
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
CREATE OR REPLACE FUNCTION zdb_tally(table_name regclass, fieldname text, stem text, query text, max_terms bigint, sort_order zdb_tally_order) RETURNS SETOF zdb_tally_response STRICT IMMUTABLE LANGUAGE sql AS $$
  SELECT zdb_tally($1, $2, false, $3, $4, $5, $6);
$$;

CREATE TYPE zdb_range_agg_response AS (key TEXT, low DOUBLE PRECISION, high DOUBLE PRECISION, doc_count INT8);
CREATE OR REPLACE FUNCTION zdb_internal_range_agg(type_oid oid, fieldname text, range_spec json, user_query text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
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


CREATE TYPE zdb_significant_terms_response AS (term text, count bigint, score float8);
CREATE OR REPLACE FUNCTION zdb_internal_significant_terms(type_oid oid, fieldname text, stem text, query text, max_terms bigint) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
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
CREATE OR REPLACE FUNCTION zdb_significant_terms(table_name regclass, fieldname text, stem text, query text, max_terms bigint) RETURNS SETOF zdb_significant_terms_response STRICT IMMUTABLE LANGUAGE sql AS $$
  SELECT zdb_significant_terms($1, $2, false, $3, $4, $5);
$$;

CREATE TYPE zdb_extended_stats_response AS (count bigint, total float8, min float8, max float8, mean float8, sum_of_squares float8, variance float8, std_deviation float8);
CREATE OR REPLACE FUNCTION zdb_internal_extended_stats(type_oid oid, fieldname text, query text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
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


CREATE DOMAIN zdb_arbitrary_aggregate_response AS json;
CREATE OR REPLACE FUNCTION zdb_internal_arbitrary_aggregate(type_oid oid, aggregate_query text, query text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_arbitrary_aggregate(table_name regclass, aggregate_query text, query text) RETURNS zdb_arbitrary_aggregate_response STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT (zdb_internal_arbitrary_aggregate(zdb_determine_index(table_name), aggregate_query, query)->'aggregations')::zdb_arbitrary_aggregate_response
$$;

/* NB:  column names are quoted to preserve case sensitivity so they exactly match the JSON property names when we call json_populate_recordset */
CREATE TYPE zdb_highlight_response AS ("primaryKey" text, "fieldName" text, "arrayIndex" int4, "term" text, "type" text, "position" int4, "startOffset" int8, "endOffset" int8, "clause" text);
CREATE OR REPLACE FUNCTION zdb_internal_highlight(type_oid oid, query json, document_json json) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_highlight(table_name regclass, es_query text, where_clause text) RETURNS SETOF zdb_highlight_response LANGUAGE plpgsql STRICT IMMUTABLE AS $$
DECLARE
    type_oid oid;
    json_data json;
    columns text;
BEGIN
    type_oid := zdb_determine_index(table_name);
    SELECT array_to_string(array_agg(attname), ',') FROM pg_attribute WHERE attrelid = table_name AND attname <> 'zdb' AND not attisdropped INTO columns;
    EXECUTE format('SELECT zdb_internal_highlight(%s, %L, json_agg(row_to_json)) FROM (SELECT row_to_json(the_table) FROM (SELECT %s FROM %s) the_table WHERE %s) x', type_oid, to_json(es_query::text), columns, table_name, where_clause) INTO json_data;

    RETURN QUERY (
        SELECT * FROM json_populate_recordset(null::zdb_highlight_response, json_data) AS highlight_data ORDER BY "primaryKey", "fieldName", "arrayIndex", "position"
    );
END;
$$;
CREATE OR REPLACE FUNCTION zdb_highlight(_table_name REGCLASS, es_query TEXT, where_clause TEXT, columns TEXT []) RETURNS SETOF zdb_highlight_response LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  type_oid     OID;
  json_data    JSON;
  columns_list TEXT;
BEGIN
  IF columns IS NULL OR columns = ARRAY[]::text[] THEN
    RETURN QUERY SELECT * FROM zdb_highlight(_table_name, es_query, where_clause);
  END IF;

  type_oid := zdb_determine_index(_table_name);

  SELECT array_to_string(array_agg(DISTINCT split_part(column_name, '.', 1)), ',') FROM (
    select column_name from information_schema.key_column_usage where (table_schema, table_name) = (select (select nspname from pg_namespace where oid = relnamespace) as schema_name, relname as table_name from pg_class where oid = (select indrelid from pg_index where indexrelid = type_oid))
     UNION
  select unnest(columns)
  ) x INTO columns_list;

  EXECUTE format('SELECT zdb_internal_highlight(%s, %L, json_agg(row_to_json)) FROM (SELECT row_to_json(the_table) FROM (SELECT %s FROM %s WHERE %s) the_table) x', type_oid, to_json(es_query :: TEXT), columns_list, _table_name, where_clause) INTO json_data;
  RETURN QUERY (
    SELECT *
    FROM json_populate_recordset(NULL :: zdb_highlight_response, json_data) AS highlight_data
    ORDER BY "primaryKey", "fieldName", "arrayIndex", "position"
  );
END;
$$;


CREATE TYPE zdb_suggest_terms_response AS (term text, count int8);
CREATE OR REPLACE FUNCTION zdb_internal_suggest_terms(typeoid oid, fieldname text, base text, query text, max_terms int8) RETURNS json LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb';
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



CREATE TYPE zdb_multi_search_response AS (table_name regclass, user_identifier text, query text, total int8, score float4[], row_data json);

CREATE OR REPLACE FUNCTION zdb_id_to_ctid(id text) RETURNS tid LANGUAGE sql STRICT IMMUTABLE AS $$
SELECT ('(' || replace(id, '-', ',') || ')')::tid;
$$;

CREATE OR REPLACE FUNCTION zdb_extract_table_row(table_name regclass, field_names text[], row_ctid tid) RETURNS json LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  is_view bool;
  pkey_column text;
  real_table_name regclass;
  row_data json;
BEGIN
  SELECT relkind = 'v' INTO is_view FROM pg_class WHERE oid = table_name;
  SELECT indrelid::regclass INTO real_table_name FROM pg_index WHERE indexrelid = zdb_determine_index(table_name);

  IF is_view THEN
    SELECT column_name
    INTO pkey_column
    FROM information_schema.key_column_usage
    WHERE (table_catalog || '.' || table_schema || '.' || information_schema.key_column_usage.table_name)::regclass = real_table_name;
  END IF;


  IF pkey_column IS NULL THEN
    /* just get what we can from the underlying table */
    EXECUTE format('SELECT row_to_json(x) FROM (SELECT %s, ctid FROM %s) x WHERE ctid = ''%s''',
                   CASE WHEN field_names IS NOT NULL THEN array_to_string(field_names, ',') ELSE (select array_to_string(array_agg(attname), ',') from pg_attribute where attrelid = real_table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0) END,
                   real_table_name,
                   row_ctid) INTO row_data;
  ELSE
    /* select out of the view */
    EXECUTE format('SELECT row_to_json(x) FROM (SELECT %s as _zdb_pkey, %s FROM %s) x WHERE _zdb_pkey = (SELECT %s FROM %s WHERE ctid = ''%s'')',
                   CASE WHEN field_names IS NOT NULL THEN array_to_string(field_names, ',') ELSE (select array_to_string(array_agg(attname), ',') from pg_attribute where attrelid = table_name and atttypid <> 'fulltext'::regtype and not attisdropped and attnum >=0) END,
                   pkey_column,
                   table_name,
                   pkey_column,
                   real_table_name,
                   row_ctid
    ) INTO row_data;
  END IF;

  RETURN row_data;
END;
$$;

CREATE OR REPLACE FUNCTION zdb_internal_multi_search(table_names oid[], queries text[]) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_multi_search(table_names regclass[], user_identifiers text[], field_names text[][], queries text[]) RETURNS SETOF zdb_multi_search_response LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
  response json;
  many integer;
BEGIN
  IF array_upper(table_names,1) <> array_upper(user_identifiers,1) OR array_upper(table_names,1) <> array_upper(queries,1) THEN
    RAISE EXCEPTION 'Arrays of table names, user_identifiers, and queries are not of the same length';
  END IF;

  response := zdb_internal_multi_search((SELECT array_agg(zdb_determine_index(unnest)) FROM unnest(table_names)), queries)->'responses';
  many := array_upper(table_names, 1);

  RETURN QUERY
  SELECT
    table_name,
    user_identifier,
    query,
    total,
    array_agg(score ORDER BY score DESC),
    json_agg(row_data ORDER BY score DESC)
  FROM (
         SELECT
           table_names[gs]                                                                                       AS table_name,
           user_identifiers[gs]                                                                                  AS user_identifier,
           queries[gs]                                                                                           AS query,
           (json_array_element(response, gs - 1) -> 'hits' ->>'total') :: INT8                                   AS total,
           (json_array_elements(json_array_element(response, gs - 1) -> 'hits' -> 'hits') ->>'_score') :: FLOAT4 AS score,
           zdb_extract_table_row(
               table_names[gs],
               (field_names[gs:gs])::text[],
               zdb_id_to_ctid(json_array_elements(json_array_element(response, gs - 1) -> 'hits' -> 'hits') ->> '_id') :: tid
           )                                                                                                     AS row_data
         FROM generate_series(1, many) gs
       ) x
  GROUP BY 1, 2, 3, 4;
END;
$$;
CREATE OR REPLACE FUNCTION zdb_multi_search(table_names regclass[], user_identifier text[], field_names text[][], query text) RETURNS SETOF zdb_multi_search_response LANGUAGE sql IMMUTABLE AS $$
SELECT * FROM zdb_multi_search($1, $2, $3, (SELECT array_agg($4) FROM unnest(table_names)));
$$;

CREATE OR REPLACE FUNCTION zdb_multi_search(table_names regclass[], user_identifier text[], query text) RETURNS SETOF zdb_multi_search_response LANGUAGE sql STRICT IMMUTABLE AS $$
SELECT * FROM zdb_multi_search($1, $2, NULL, (SELECT array_agg($3) FROM unnest(table_names)));
$$;


CREATE TYPE zdb_termlist_response AS (
  term text,
  totalfreq bigint,
  docfreq bigint
);
CREATE OR REPLACE FUNCTION zdb_internal_termlist(indexrel oid, fieldname text, prefix text, startat text, size int4) RETURNS json IMMUTABLE LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_termlist(table_name regclass, fieldname text, prefix text, startat text, size int4) RETURNS SETOF zdb_termlist_response IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
  indexRel oid;
  response json;
BEGIN
  indexRel := zdb_determine_index(table_name);
  response := zdb_internal_termlist(indexRel, fieldname, prefix, startat::TEXT, size);

  RETURN QUERY SELECT
                 (value->>'term') AS term,
                 (value->>'totalfreq')::bigint AS totalfreq,
                 (value->>'docfreq')::bigint AS docfreq
               FROM json_array_elements(response->'terms') value
               LIMIT CASE WHEN size > 0 THEN size ELSE 2147483647 END;
END;
$$;

DO LANGUAGE plpgsql $$
BEGIN
    PERFORM * FROM pg_am WHERE amname = 'zombodb';
    IF NOT FOUND THEN
        -- we don't really care what these values are, because we always update them below
        INSERT INTO pg_am (amname, amstrategies, amsupport, amcanorder, amcanorderbyop, amcanbackward, amcanunique, amcanmulticol, amoptionalkey, amsearcharray, amsearchnulls, amstorage, amclusterable, ampredlocks, amkeytype, aminsert, ambeginscan, amgettuple, amgetbitmap, amrescan, amendscan, ammarkpos, amrestrpos, ambuild, ambuildempty, ambulkdelete, amvacuumcleanup, amcanreturn, amcostestimate, amoptions)
        VALUES           ('zombodb', 1, 1, 'f', 'f', 'f', 'f', 't', 'f', 'f', 't', 't', 'f', 'f', 0, 'zdbinsert', 'zdbbeginscan', 'zdbgettuple', '-', 'zdbrescan', 'zdbendscan', 'zdbmarkpos', 'zdbrestrpos', 'zdbbuild', 'zdbbuildempty', 'zdbbulkdelete', 'zdbvacuumcleanup', '-', 'zdbcostestimate', 'zdboptions');
    END IF;

    UPDATE pg_am SET
        amname = 'zombodb',
        amstrategies = '1',
        amsupport = '1',
        amcanorder = 'f',
        amcanorderbyop = 'f',
        amcanbackward = 'f',
        amcanunique = 'f',
        amcanmulticol = 't',
        amoptionalkey = 'f',
        amsearcharray = 'f',
        amsearchnulls = 't',
        amstorage = 't',
        amclusterable = 'f',
        ampredlocks = 'f',
        amkeytype = '0',
        aminsert = 'zdbinsert',
        ambeginscan = 'zdbbeginscan',
        amgettuple = 'zdbgettuple',
        amgetbitmap = '-',
        amrescan = 'zdbrescan',
        amendscan = 'zdbendscan',
        ammarkpos = 'zdbmarkpos',
        amrestrpos = 'zdbrestrpos',
        ambuild = 'zdbbuild',
        ambuildempty = 'zdbbuildempty',
        ambulkdelete = 'zdbbulkdelete',
        amvacuumcleanup = 'zdbvacuumcleanup',
        amcanreturn = '-',
        amcostestimate = 'zdbcostestimate',
        amoptions = 'zdboptions'
    WHERE amname = 'zombodb';

    RETURN;
END;
$$;

CREATE OPERATOR ==> (
    PROCEDURE = zdb_query_func,
    LEFTARG = json,
    RIGHTARG = text
);

CREATE OPERATOR CLASS zombodb_json_ops DEFAULT FOR TYPE json USING zombodb AS STORAGE json;

CREATE OPERATOR ==> (
    PROCEDURE = zdb_tid_query_func,
    RESTRICT = zdbsel,
    LEFTARG = tid,
    RIGHTARG = text
);

CREATE OPERATOR CLASS zombodb_tid_ops DEFAULT FOR TYPE tid USING zombodb AS
    OPERATOR 1 ==>(tid, text),
    FUNCTION 1 zdb_tid_query_func(tid, text),
    STORAGE json;


--
-- filter/analyzer/mapping support
--

CREATE TABLE zdb_filters (
  name text NOT NULL PRIMARY KEY,
  definition json NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb_char_filters (
  name text NOT NULL PRIMARY KEY,
  definition json NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb_analyzers (
  name text NOT NULL PRIMARY KEY,
  definition json NOT NULL,
  is_default boolean DEFAULT false NOT NULL
);

CREATE TABLE zdb_mappings (
  table_name regclass NOT NULL,
  field_name name NOT NULL,
  definition json NOT NULL,
  PRIMARY KEY (table_name, field_name)
);

SELECT pg_catalog.pg_extension_config_dump('zdb_filters', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb_char_filters', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb_analyzers', 'WHERE NOT is_default');
SELECT pg_catalog.pg_extension_config_dump('zdb_mappings', '');

CREATE OR REPLACE FUNCTION zdb_define_filter(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb_filters WHERE name = $1;
  INSERT INTO zdb_filters(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb_define_char_filter(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb_char_filters WHERE name = $1;
  INSERT INTO zdb_char_filters(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb_define_analyzer(name text, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb_analyzers WHERE name = $1;
  INSERT INTO zdb_analyzers(name, definition) VALUES ($1, $2);
$$;

CREATE OR REPLACE FUNCTION zdb_define_mapping(table_name regclass, field_name name, definition json) RETURNS void LANGUAGE sql VOLATILE STRICT AS $$
  DELETE FROM zdb_mappings WHERE table_name = $1 AND field_name = $2;
  INSERT INTO zdb_mappings(table_name, field_name, definition) VALUES ($1, $2, $3);
$$;

INSERT INTO zdb_filters(name, definition, is_default) VALUES (
  'zdb_truncate_32000', '{
          "type": "truncate",
          "length": 32000
        }', true);

INSERT INTO zdb_analyzers(name, definition, is_default) VALUES (
  'default', '{
          "tokenizer": "keyword",
          "filter": ["trim", "zdb_truncate_32000", "lowercase"]
        }', true);
INSERT INTO zdb_analyzers(name, definition, is_default) VALUES (
  'exact', '{
          "tokenizer": "keyword",
          "filter": ["trim", "zdb_truncate_32000", "lowercase"]
        }', true);
INSERT INTO zdb_analyzers(name, definition, is_default) VALUES (
  'phrase', '{
          "tokenizer": "standard",
          "filter": ["lowercase"]
        }', true);
INSERT INTO zdb_analyzers(name, definition, is_default) VALUES (
  'fulltext', '{
          "tokenizer": "standard",
          "filter": ["lowercase"]
        }', true);

CREATE DOMAIN arabic AS text;
CREATE DOMAIN armenian AS text;
CREATE DOMAIN basque AS text;
CREATE DOMAIN brazilian AS text;
CREATE DOMAIN bulgarian AS text;
CREATE DOMAIN catalan AS text;
CREATE DOMAIN chinese AS text;
CREATE DOMAIN cjk AS text;
CREATE DOMAIN czech AS text;
CREATE DOMAIN danish AS text;
CREATE DOMAIN dutch AS text;
CREATE DOMAIN english AS text;
CREATE DOMAIN finnish AS text;
CREATE DOMAIN french AS text;
CREATE DOMAIN galician AS text;
CREATE DOMAIN german AS text;
CREATE DOMAIN greek AS text;
CREATE DOMAIN hindi AS text;
CREATE DOMAIN hungarian AS text;
CREATE DOMAIN indonesian AS text;
CREATE DOMAIN irish AS text;
CREATE DOMAIN italian AS text;
CREATE DOMAIN latvian AS text;
CREATE DOMAIN norwegian AS text;
CREATE DOMAIN persian AS text;
CREATE DOMAIN portuguese AS text;
CREATE DOMAIN romanian AS text;
CREATE DOMAIN russian AS text;
CREATE DOMAIN sorani AS text;
CREATE DOMAIN spanish AS text;
CREATE DOMAIN swedish AS text;
CREATE DOMAIN turkish AS text;
CREATE DOMAIN thai AS text;

CREATE TYPE zdb_analyze_text_response AS (token text, start_offset integer, end_offset integer, type text, position integer);
CREATE OR REPLACE FUNCTION zdb_internal_analyze_text(index_name regclass, analyzer_name text, data text) RETURNS json IMMUTABLE STRICT LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_analyze_text(index_name regclass, analyzer_name text, data text) RETURNS SETOF zdb_analyze_text_response IMMUTABLE STRICT LANGUAGE plpgsql AS $$
DECLARE
    results json;
BEGIN
    results := zdb_internal_analyze_text(index_name, analyzer_name, data);
    RETURN QUERY SELECT (value->>'token')::text,
                        (value->>'start_offset')::integer,
                        (value->>'end_offset')::integer,
                        (value->>'type')::text,
                        (value->>'position')::integer
                   FROM json_array_elements(results->'tokens');
END;
$$;
