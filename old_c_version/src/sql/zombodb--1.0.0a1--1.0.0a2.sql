CREATE OR REPLACE FUNCTION llapi_direct_insert(index_name regclass, data json) RETURNS void LANGUAGE c AS 'MODULE_PATHNAME', 'llapi_direct_insert';
CREATE OR REPLACE FUNCTION llapi_direct_delete(index_name regclass, _id text) RETURNS void LANGUAGE c AS 'MODULE_PATHNAME', 'llapi_direct_delete';

CREATE OR REPLACE FUNCTION top_hits(index regclass, fields text[], query zdbquery, size int) RETURNS TABLE (ctid tid, score float4, source json) STABLE LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_top_hits(index, fields, query, size)::jsonb;
BEGIN
    RETURN QUERY
        SELECT zdb.ctid((hit->>'_id')::bigint),
               (hit->>'_score')::float4,
               jsonb_pretty(hit->'_source')::json
          FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'hits'->'hits') hit;
END;
$$;

CREATE OR REPLACE FUNCTION top_hits_with_id(index regclass, fields text[], query zdbquery, size int) RETURNS TABLE (_id text, score float4, source json) STABLE LANGUAGE plpgsql AS $$
DECLARE
    response jsonb := zdb.internal_top_hits(index, fields, query, size)::jsonb;
BEGIN
    RETURN QUERY
        SELECT hit->>'_id',
               (hit->>'_score')::float4,
               jsonb_pretty(hit->'_source')::json
          FROM jsonb_array_elements(response->'aggregations'->'the_agg'->'hits'->'hits') hit;
END;
$$;
