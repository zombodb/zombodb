CREATE DOMAIN zdb_json_aggregate_response AS json;
CREATE OR REPLACE FUNCTION zdb_internal_json_aggregate(type_oid oid, json_agg json, query text) RETURNS json LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_json_aggregate(table_name regclass, json_agg json, query text) RETURNS zdb_json_aggregate_response STRICT IMMUTABLE LANGUAGE sql AS $$
    SELECT (zdb_internal_json_aggregate(zdb_determine_index(table_name), json_agg, query)->'aggregations')::zdb_json_aggregate_response
$$;
