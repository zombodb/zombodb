CREATE OR REPLACE FUNCTION tid_cmpfunc(tid, zdbquery) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_tid_cmpfunc';
CREATE OR REPLACE FUNCTION tid_cmpfunc_array_should(tid, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_tid_cmpfunc_array_should';
CREATE OR REPLACE FUNCTION tid_cmpfunc_array_must(tid, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_tid_cmpfunc_array_must';
CREATE OR REPLACE FUNCTION tid_cmpfunc_array_not(tid, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_tid_cmpfunc_array_not';

CREATE OPERATOR pg_catalog.==> (
    PROCEDURE = zdb.tid_cmpfunc,
    RESTRICT = restrict,
    LEFTARG = tid,
    RIGHTARG = zdbquery
);
COMMENT ON OPERATOR pg_catalog.==>(tid, zdbquery) IS 'ZomboDB text search operator for Elasticsearch queries';

CREATE OPERATOR pg_catalog.==| (
    PROCEDURE = zdb.tid_cmpfunc_array_should,
    RESTRICT = restrict,
    LEFTARG = tid,
    RIGHTARG = zdbquery[]
);
COMMENT ON OPERATOR pg_catalog.==|(tid, zdbquery[]) IS 'ZomboDB array "should" text search operator for Elasticsearch queries';

CREATE OPERATOR pg_catalog.==& (
    PROCEDURE = zdb.tid_cmpfunc_array_must,
    RESTRICT = restrict,
    LEFTARG = tid,
    RIGHTARG = zdbquery[]
);
COMMENT ON OPERATOR pg_catalog.==&(tid, zdbquery[]) IS 'ZomboDB array "must" text search operator for Elasticsearch queries';

CREATE OPERATOR pg_catalog.==! (
    PROCEDURE = zdb.tid_cmpfunc_array_not,
    RESTRICT = restrict,
    LEFTARG = tid,
    RIGHTARG = zdbquery[]
);
COMMENT ON OPERATOR pg_catalog.==!(tid, zdbquery[]) IS 'ZomboDB array "not" text search operator for Elasticsearch queries';

CREATE OPERATOR CLASS tid_text_ops DEFAULT FOR TYPE tid USING zombodb AS
    OPERATOR 1 pg_catalog.==>(tid, zdbquery),
    OPERATOR 2 pg_catalog.==|(tid, zdbquery[]),
    OPERATOR 3 pg_catalog.==&(tid, zdbquery[]),
    OPERATOR 4 pg_catalog.==!(tid, zdbquery[]),
    STORAGE tid;


ALTER FUNCTION score(tid) PARALLEL UNSAFE;
ALTER FUNCTION highlight(tid, name, json) PARALLEL UNSAFE;
ALTER FUNCTION highlight(zdb.esqdsl_highlight_type, boolean, int, zdbquery, text[], text[], text, int, zdb.esqdsl_fragmenter_type, int, int, boolean, zdb.esqdsl_encoder_type, text, int, text, int, boolean, text) PARALLEL UNSAFE;