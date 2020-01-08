CREATE OR REPLACE FUNCTION llapi_direct_insert(index_name regclass, data json) RETURNS void LANGUAGE c AS 'MODULE_PATHNAME', 'llapi_direct_insert';
CREATE OR REPLACE FUNCTION llapi_direct_delete(index_name regclass, _id text) RETURNS void LANGUAGE c AS 'MODULE_PATHNAME', 'llapi_direct_delete';
