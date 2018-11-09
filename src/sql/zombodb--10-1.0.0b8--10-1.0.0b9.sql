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

CREATE OPERATOR CLASS tid_zdb_ops DEFAULT FOR TYPE tid USING zombodb AS
    OPERATOR 1 pg_catalog.==>(tid, zdbquery),
    OPERATOR 2 pg_catalog.==|(tid, zdbquery[]),
    OPERATOR 3 pg_catalog.==&(tid, zdbquery[]),
    OPERATOR 4 pg_catalog.==!(tid, zdbquery[]),
    STORAGE tid;

-- rename the opclass
UPDATE pg_opclass SET opcname = 'anyelement_zdb_ops' WHERE opcname = 'anyelement_text_ops';

-- some functions need to be parallel unsafe
ALTER FUNCTION score(tid) PARALLEL UNSAFE;
ALTER FUNCTION highlight(tid, name, json) PARALLEL UNSAFE;
ALTER FUNCTION highlight(zdb.esqdsl_highlight_type, boolean, int, zdbquery, text[], text[], text, int, zdb.esqdsl_fragmenter_type, int, int, boolean, zdb.esqdsl_encoder_type, text, int, text, int, boolean, text) PARALLEL UNSAFE;

--
-- upgrade existing zombodb index definitions
--
DO LANGUAGE plpgsql $$
DECLARE
    r record;
BEGIN
    FOR r IN (SELECT oid FROM pg_class WHERE relam = (SELECT oid FROM pg_am WHERE amname = 'zombodb')) LOOP
        RAISE NOTICE 'Upgrading index definition for %', r.oid::regclass;

        RAISE NOTICE '...pg_index';
        UPDATE pg_index
            SET
                indnatts = 2,
                indkey = '-1 0'::int2vector,
                indcollation = '0 0'::oidvector,
                indclass = ((SELECT oid FROM pg_opclass WHERE opcname = 'tid_zdb_ops') || ' ' || (SELECT oid FROM pg_opclass WHERE opcname = 'anyelement_zdb_ops'))::oidvector
            WHERE
                indexrelid = r.oid;

        RAISE NOTICE '...pg_class';
        UPDATE pg_class
            SET
                relnatts = 2
            WHERE
                oid = r.oid;

        RAISE NOTICE '...pg_attribute';
        UPDATE pg_attribute
            SET
                attnum = 2
            WHERE
                attrelid = r.oid AND attnum = 1;

        RAISE NOTICE '...pg_attribute new row';
        INSERT INTO pg_attribute (
            attrelid,
            attname,
            atttypid,
            attstattarget,
            attlen,
            attnum,
            attndims,
            attcacheoff,
            atttypmod,
            attbyval,
            attstorage,
            attalign,
            attnotnull,
            atthasdef,
            attidentity,
            attisdropped,
            attislocal,
            attinhcount,
            attcollation,
            attacl,
            attoptions,
            attfdwoptions
        )
        VALUES (
            r.oid, /* attrelid */
            'ctid', /* attname */
            'tid'::regtype::oid, /* atttypid */
            -1, /* attstattarget */
            6, /* attlen */
            1, /* attnum */
            0, /* attndims */
            -1, /* attcacheoff */
            -1, /* atttypmod */
            false, /* attbyval */
            'p', /* attstorage */
            's', /* attalign */
            false, /* attnotnull */
            false, /* atthasdef */
            '', /* attidentity */
            false, /* attisdropped */
            true, /* attislocal */
            0, /* attinhcount */
            0, /* attcollation */
            NULL, /* attacl */
            NULL, /* attoptions */
            NULL /* attfdwoptions */
        );
        RAISE NOTICE '   Upgraded %s''s definition to: %s', r.oid::regclass, pg_get_indexdef(r.oid);
    END LOOP;
END;
$$;