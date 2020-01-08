--
-- access method API functions and DDL
--
CREATE OR REPLACE FUNCTION amhandler(internal) RETURNS index_am_handler PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_amhandler';
CREATE OR REPLACE FUNCTION anyelement_cmpfunc(anyelement, zdbquery) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_anyelement_cmpfunc';
CREATE OR REPLACE FUNCTION anyelement_cmpfunc_array_should(anyelement, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_anyelement_cmpfunc_array_should';
CREATE OR REPLACE FUNCTION anyelement_cmpfunc_array_must(anyelement, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_anyelement_cmpfunc_array_must';
CREATE OR REPLACE FUNCTION anyelement_cmpfunc_array_not(anyelement, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_anyelement_cmpfunc_array_not';

CREATE OR REPLACE FUNCTION tid_cmpfunc(tid, zdbquery) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_tid_cmpfunc';
CREATE OR REPLACE FUNCTION tid_cmpfunc_array_should(tid, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_tid_cmpfunc_array_should';
CREATE OR REPLACE FUNCTION tid_cmpfunc_array_must(tid, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_tid_cmpfunc_array_must';
CREATE OR REPLACE FUNCTION tid_cmpfunc_array_not(tid, zdbquery[]) RETURNS bool PARALLEL SAFE IMMUTABLE STRICT COST 0.0001 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_tid_cmpfunc_array_not';

CREATE OR REPLACE FUNCTION restrict(internal, oid, internal, integer) RETURNS float8 PARALLEL SAFE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_restrict';
CREATE OR REPLACE FUNCTION delete_trigger() RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_delete_trigger';
CREATE OR REPLACE FUNCTION update_trigger() RETURNS trigger LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_update_trigger';

CREATE ACCESS METHOD zombodb TYPE INDEX HANDLER zdb.amhandler;

CREATE OPERATOR pg_catalog.==> (
    PROCEDURE = zdb.anyelement_cmpfunc,
    RESTRICT = restrict,
    LEFTARG = anyelement,
    RIGHTARG = zdbquery
);
COMMENT ON OPERATOR pg_catalog.==>(anyelement, zdbquery) IS 'ZomboDB text search operator for Elasticsearch queries';

CREATE OPERATOR pg_catalog.==| (
    PROCEDURE = zdb.anyelement_cmpfunc_array_should,
    RESTRICT = restrict,
    LEFTARG = anyelement,
    RIGHTARG = zdbquery[]
);
COMMENT ON OPERATOR pg_catalog.==|(anyelement, zdbquery[]) IS 'ZomboDB array "should" text search operator for Elasticsearch queries';

CREATE OPERATOR pg_catalog.==& (
    PROCEDURE = zdb.anyelement_cmpfunc_array_must,
    RESTRICT = restrict,
    LEFTARG = anyelement,
    RIGHTARG = zdbquery[]
);
COMMENT ON OPERATOR pg_catalog.==&(anyelement, zdbquery[]) IS 'ZomboDB array "must" text search operator for Elasticsearch queries';

CREATE OPERATOR pg_catalog.==! (
    PROCEDURE = zdb.anyelement_cmpfunc_array_not,
    RESTRICT = restrict,
    LEFTARG = anyelement,
    RIGHTARG = zdbquery[]
);
COMMENT ON OPERATOR pg_catalog.==!(anyelement, zdbquery[]) IS 'ZomboDB array "not" text search operator for Elasticsearch queries';

CREATE OPERATOR CLASS anyelement_zdb_ops DEFAULT FOR TYPE anyelement USING zombodb AS
    OPERATOR 1 pg_catalog.==>(anyelement, zdbquery),
    OPERATOR 2 pg_catalog.==|(anyelement, zdbquery[]),
    OPERATOR 3 pg_catalog.==&(anyelement, zdbquery[]),
    OPERATOR 4 pg_catalog.==!(anyelement, zdbquery[]),
    STORAGE anyelement;

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

CREATE OPERATOR CLASS tid_zdb_ops DEFAULT FOR TYPE tid USING zombodb AS
    OPERATOR 1 pg_catalog.==>(tid, zdbquery),
    OPERATOR 2 pg_catalog.==|(tid, zdbquery[]),
    OPERATOR 3 pg_catalog.==&(tid, zdbquery[]),
    OPERATOR 4 pg_catalog.==!(tid, zdbquery[]),
    STORAGE tid;
