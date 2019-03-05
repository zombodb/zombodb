ALTER FUNCTION version() STABLE;
ALTER FUNCTION request(index regclass, endpoint text, method text, post_data text) STABLE;
ALTER FUNCTION index_name(index regclass) STABLE;
ALTER FUNCTION index_url(index regclass) STABLE;
ALTER FUNCTION index_type_name(index regclass) STABLE;
ALTER FUNCTION index_mapping(index regclass) STABLE;
ALTER FUNCTION all_es_index_names() STABLE;

ALTER FUNCTION query(index regclass, query zdbquery) STABLE;
ALTER FUNCTION query_raw(index regclass, query zdbquery) STABLE;
ALTER FUNCTION query_tids(index regclass, query zdbquery) STABLE;
ALTER FUNCTION profile_query(index regclass, query zdbquery) STABLE;
ALTER FUNCTION zdb.set_query_property(property text, value text, query zdbquery) STABLE;
