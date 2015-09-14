CREATE TYPE zdb_range_agg_response AS (key TEXT, low DOUBLE PRECISION, high DOUBLE PRECISION, doc_count INT8);
CREATE OR REPLACE FUNCTION zdb_internal_range_agg(type_oid oid, fieldname text, range_spec json, user_query text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_range_agg(table_name regclass, fieldname text, range_spec json, user_query text) RETURNS SETOF zdb_range_agg_response LANGUAGE plpgsql STRICT IMMUTABLE AS $$
DECLARE
  nested boolean;
  response json;
  buckets json;
BEGIN
  response := zdb_internal_range_agg(zdb_determine_index(table_name), fieldname, range_spec, user_query);
  nested := (response->'aggregations'->fieldname->fieldname->fieldname->'buckets') IS NOT NULL;

  IF nested THEN
      buckets := response->'aggregations'->fieldname->fieldname->fieldname->'buckets';
  ELSE
      buckets := response->'aggregations'->fieldname->'buckets';
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


CREATE OR REPLACE FUNCTION zdb(table_name regclass, ctid tid) RETURNS tid LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb', 'zdb_table_ref_and_tid';
CREATE OR REPLACE FUNCTION zdb_tid_query_func(tid, text) RETURNS bool LANGUAGE c STRICT AS '$libdir/plugins/zombodb' COST 0.000001;

CREATE OPERATOR ==> (
    PROCEDURE = zdb_tid_query_func,
    RESTRICT = zdbsel,
    LEFTARG = tid,
    RIGHTARG = text,
    HASHES, MERGES
);

CREATE OPERATOR CLASS zombodb_tid_ops DEFAULT FOR TYPE tid USING zombodb AS
    OPERATOR 1 ==>(tid, text),
    FUNCTION 1 zdb_tid_query_func(tid, text),
    STORAGE json;