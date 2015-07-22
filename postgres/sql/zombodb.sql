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
CREATE OR REPLACE FUNCTION zdbgetbitmap(internal, internal) RETURNS bigint LANGUAGE c STRICT AS '$libdir/plugins/zombodb';
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

CREATE OR REPLACE FUNCTION zdb_query_func(json, text) RETURNS bool LANGUAGE c STRICT AS '$libdir/plugins/zombodb' COST 2147483647;
CREATE OR REPLACE FUNCTION zdb(record) RETURNS json LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb', 'zdb_row_to_json';

CREATE OR REPLACE FUNCTION zdbtupledeletedtrigger() RETURNS trigger AS '$libdir/plugins/zombodb' language c;
CREATE OR REPLACE FUNCTION zdbeventtrigger() RETURNS event_trigger AS '$libdir/plugins/zombodb' language c;

CREATE EVENT TRIGGER zdb_alter_table_trigger ON ddl_command_end WHEN TAG IN ('ALTER TABLE') EXECUTE PROCEDURE zdbeventtrigger();

CREATE OR REPLACE FUNCTION rest_get(url text) RETURNS json AS '$libdir/plugins/zombodb' language c;
CREATE OR REPLACE FUNCTION zdb_get_index_name(index_name regclass) RETURNS text AS '$libdir/plugins/zombodb' language c;
CREATE OR REPLACE FUNCTION zdb_get_url(index_name regclass) RETURNS text AS '$libdir/plugins/zombodb' language c;
CREATE OR REPLACE FUNCTION zdb_num_hits() RETURNS int8 AS '$libdir/plugins/zombodb' language c;

CREATE OR REPLACE FUNCTION count_of_table(table_name REGCLASS) RETURNS INT8 LANGUAGE plpgsql AS $$
DECLARE
  cnt INT8;
BEGIN
  EXECUTE format('SELECT count(*) FROM %I', table_name)
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

CREATE OR REPLACE FUNCTION zdb_determine_index(table_name regclass) RETURNS oid STRICT VOLATILE LANGUAGE plpgsql AS $$
DECLARE
  exp json;
  kind char;
BEGIN
    SELECT relkind INTO kind FROM pg_class WHERE oid = table_name::oid;

    IF kind = 'r' THEN
      EXECUTE format('EXPLAIN (FORMAT JSON) SELECT 1 FROM %s x WHERE zdb(x) ==> '''' ', table_name) INTO exp;
    ELSE
      EXECUTE format('EXPLAIN (FORMAT JSON) SELECT 1 FROM %s WHERE zdb ==> '''' ', table_name) INTO exp;
    END IF;

    IF (json_array_element(exp, 0)->'Plan'->'Plans')::text IS NOT NULL THEN
      RETURN oid FROM pg_class WHERE relname IN (SELECT json_array_elements(json_array_element(exp, 0)->'Plan'->'Plans')->>'Index Name' AS index_name)
                                 AND relam = (SELECT oid FROM pg_am WHERE amname = 'zombodb');
    END IF;

    RETURN (json_array_element(exp, 0)->'Plan'->>'Index Name')::regclass::oid;
END;
$$;

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

CREATE TYPE zdb_tally_order AS ENUM ('count', 'term', 'reverse_count', 'reverse_term');
CREATE TYPE zdb_tally_response AS (term text, count bigint);
CREATE OR REPLACE FUNCTION zdb_internal_tally(type_oid oid, fieldname text, stem text, query text, max_terms bigint, sort_order text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_tally(table_name regclass, fieldname text, stem text, query text, max_terms bigint, sort_order zdb_tally_order) RETURNS SETOF zdb_tally_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
    json_data json;
    type_oid oid;
    missing bigint;
    nested boolean;
    new_query text;
    data_type text;
BEGIN
    type_oid := zdb_determine_index(table_name);

    SELECT typname FROM pg_type WHERE oid = (SELECT atttypid FROM pg_attribute WHERE attrelid = table_name AND attname = fieldname) INTO data_type;
    IF stem <> '^.*' AND data_type IN ('text', '_text', 'phrase', 'phrase_array', 'fulltext', 'varchar', '_varchar') THEN
      new_query := format('(%s) AND (%s:~"%s")', query, fieldname, split_part(stem, '^', 2));
    ELSE
      new_query := query;
    END IF;

    json_data := zdb_internal_tally(type_oid, fieldname, stem, new_query, max_terms, sort_order::text);
    missing := (json_data->'aggregations'->'missing'->>'doc_count')::bigint;
    nested := (json_data->'aggregations'->fieldname->fieldname->fieldname->'buckets') IS NOT NULL;

    IF nested THEN
        missing := (json_data->'aggregations'->fieldname->fieldname->'missing'->>'doc_count')::bigint;
        json_data := json_data->'aggregations'->fieldname->fieldname->fieldname->'buckets';
    ELSE
        missing := (json_data->'aggregations'->'missing'->>'doc_count')::bigint;
        json_data := json_data->'aggregations'->fieldname->'buckets';
    END IF;

    IF missing IS NULL OR missing = 0 THEN
      RETURN QUERY (
          SELECT
              coalesce(upper((x->>'key_as_string')::text), upper((x->>'key')::text)),
              (x->>'doc_count')::int8
           FROM json_array_elements(json_data) x
      );
    ELSE
      RETURN QUERY (
          SELECT * FROM (SELECT NULL::text, missing LIMIT missing) x
              UNION ALL
          SELECT
              coalesce(upper((x->>'key_as_string')::text), upper((x->>'key')::text)),
              (x->>'doc_count')::int8
           FROM json_array_elements(json_data) x
      );
    END IF;
END;
$$;

CREATE TYPE zdb_significant_terms_response AS (term text, count bigint, score float8);
CREATE OR REPLACE FUNCTION zdb_internal_significant_terms(type_oid oid, fieldname text, stem text, query text, max_terms bigint) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_significant_terms(table_name regclass, fieldname text, stem text, query text, max_terms bigint) RETURNS SETOF zdb_significant_terms_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
    json_data json;
    type_oid oid;
    nested boolean;
BEGIN
    type_oid := zdb_determine_index(table_name);
    json_data := zdb_internal_significant_terms(type_oid, fieldname, stem, query, max_terms);
    nested := (json_data->'aggregations'->fieldname->fieldname->fieldname->'buckets') IS NOT NULL;

    IF nested THEN
        json_data := json_data->'aggregations'->fieldname->fieldname->fieldname->'buckets';
    ELSE
        json_data := json_data->'aggregations'->fieldname->'buckets';
    END IF;

    RETURN QUERY (
        SELECT
            upper((x->>'key')::text),
            (x->>'doc_count')::int8,
            (x->>'score')::float8
         FROM json_array_elements(json_data) x
    );
END;
$$;

CREATE TYPE zdb_extended_stats_response AS (count bigint, total float8, min float8, max float8, mean float8, sum_of_squares float8, variance float8, std_deviation float8);
CREATE OR REPLACE FUNCTION zdb_internal_extended_stats(type_oid oid, fieldname text, query text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_extended_stats(table_name regclass, fieldname text, query text) RETURNS SETOF zdb_extended_stats_response STRICT IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
    json_data json;
    type_oid oid;
    missing bigint;
    nested boolean;
BEGIN
    type_oid := zdb_determine_index(table_name);
    json_data := zdb_internal_extended_stats(type_oid, fieldname, query);
    missing := (json_data->'aggregations'->'missing'->>'doc_count')::bigint;
    nested := (json_data->'aggregations'->fieldname->fieldname->fieldname) IS NOT NULL;

    IF nested THEN
        json_data := json_data->'aggregations'->fieldname->fieldname->fieldname;
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

  RETURN QUERY SELECT upper(base), zdb_estimate_count(table_name, fieldname || ':("' || coalesce(case when trim(base) = '' then null else trim(base) end, 'null') || '") AND (' || coalesce(query, '') || ')')
               UNION ALL
               SELECT upper((x->>'text')::text),
                 (x->>'freq')::int8
               FROM json_array_elements(json_array_element(data->'suggest'->'suggestions', 0)->'options') x;
END;
$$;

DO LANGUAGE plpgsql $$
BEGIN
    PERFORM * FROM pg_am WHERE amname = 'zombodb';
    IF NOT FOUND THEN
        -- we don't really care what these values are, because we always update them below
        INSERT INTO pg_am (amname, amstrategies, amsupport, amcanorder, amcanorderbyop, amcanbackward, amcanunique, amcanmulticol, amoptionalkey, amsearcharray, amsearchnulls, amstorage, amclusterable, ampredlocks, amkeytype, aminsert, ambeginscan, amgettuple, amgetbitmap, amrescan, amendscan, ammarkpos, amrestrpos, ambuild, ambuildempty, ambulkdelete, amvacuumcleanup, amcanreturn, amcostestimate, amoptions)
        VALUES           ('zombodb', 1, 1, 'f', 'f', 'f', 'f', 't', 'f', 'f', 't', 't', 'f', 'f', 0, 'zdbinsert', 'zdbbeginscan', 'zdbgettuple', 'zdbgetbitmap', 'zdbrescan', 'zdbendscan', 'zdbmarkpos', 'zdbrestrpos', 'zdbbuild', 'zdbbuildempty', 'zdbbulkdelete', 'zdbvacuumcleanup', '-', 'zdbcostestimate', 'zdboptions');
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
        amgetbitmap = 'zdbgetbitmap',
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
    RESTRICT = zdbsel,
    LEFTARG = json,
    RIGHTARG = text,
    HASHES, MERGES
);

CREATE OPERATOR CLASS zombodb_ops DEFAULT FOR TYPE json USING zombodb AS
    OPERATOR 1 ==>(json, text),
    FUNCTION 1 zdb_query_func(json, text),
    STORAGE json;