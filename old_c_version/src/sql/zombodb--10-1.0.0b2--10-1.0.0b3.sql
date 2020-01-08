COMMENT ON OPERATOR pg_catalog.==>(anyelement, zdbquery) IS 'ZomboDB text search operator for Elasticsearch queries';
COMMENT ON OPERATOR pg_catalog.==|(anyelement, zdbquery[]) IS 'ZomboDB array "should" text search operator for Elasticsearch queries';
COMMENT ON OPERATOR pg_catalog.==&(anyelement, zdbquery[]) IS 'ZomboDB array "must" text search operator for Elasticsearch queries';
COMMENT ON OPERATOR pg_catalog.==!(anyelement, zdbquery[]) IS 'ZomboDB array "not" text search operator for Elasticsearch queries';
