--
-- misc functions
--
CREATE OR REPLACE FUNCTION version() RETURNS text PARALLEL SAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_version';
CREATE OR REPLACE FUNCTION ctid(ctid_as_64bits bigint) RETURNS tid PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_ctid';

--
-- for making arbitrary requests to the ES cluster backing the specified index
--
CREATE OR REPLACE FUNCTION request(index regclass, endpoint text, method text DEFAULT 'GET', post_data text DEFAULT NULL) RETURNS text PARALLEL SAFE STABLE LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_request';

--
-- support functions
--
CREATE OR REPLACE FUNCTION index_name(index regclass) RETURNS text PARALLEL SAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_index_name';
CREATE OR REPLACE FUNCTION index_url(index regclass) RETURNS text PARALLEL SAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_index_url';
CREATE OR REPLACE FUNCTION index_type_name(index regclass) RETURNS text PARALLEL SAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_index_type_name';
CREATE OR REPLACE FUNCTION index_mapping(index regclass) RETURNS json PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
    SELECT (zdb.request(index, '_mapping?pretty')::json)->zdb.index_name(index);
$$;
CREATE OR REPLACE FUNCTION all_es_index_names() RETURNS SETOF text PARALLEL SAFE STABLE STRICT LANGUAGE sql AS $$
    SELECT zdb.index_name(oid::regclass) FROM pg_class WHERE relam = (SELECT oid FROM pg_am WHERE amname = 'zombodb');
$$;
