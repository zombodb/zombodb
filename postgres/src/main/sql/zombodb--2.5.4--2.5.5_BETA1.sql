CREATE OR REPLACE FUNCTION zdb_score_internal(table_name regclass, ctid tid) RETURNS float4 LANGUAGE c IMMUTABLE STRICT AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_score(table_name regclass, ctid tid) RETURNS float4 LANGUAGE sql IMMUTABLE STRICT AS $$
    SELECT zdb_score_internal(zdb_determine_index($1), $2);
$$;

CREATE OR REPLACE FUNCTION zdb_determine_index(table_name regclass) RETURNS oid LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
