--
-- ZomboDB's "zdbquery" type definition
--
CREATE TYPE pg_catalog.zdbquery;
CREATE OR REPLACE FUNCTION pg_catalog.zdbquery_in(cstring) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_in';
CREATE OR REPLACE FUNCTION pg_catalog.zdbquery_out(zdbquery) RETURNS cstring PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_out';
CREATE OR REPLACE FUNCTION pg_catalog.zdbquery_recv(internal) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_recv';
CREATE OR REPLACE FUNCTION pg_catalog.zdbquery_send(zdbquery) RETURNS bytea PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_send';
CREATE TYPE pg_catalog.zdbquery (
    INTERNALLENGTH = variable,
    INPUT = pg_catalog.zdbquery_in,
    OUTPUT = pg_catalog.zdbquery_out,
    RECEIVE = pg_catalog.zdbquery_recv,
    SEND = pg_catalog.zdbquery_send,
    ALIGNMENT = int4,
    STORAGE = extended
);

CREATE OR REPLACE FUNCTION zdbquery_from_text(text) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_from_text';
CREATE OR REPLACE FUNCTION zdbquery_from_json(json) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_from_text'; /* NB:  same C func as above */
CREATE OR REPLACE FUNCTION zdbquery_from_jsonb(jsonb) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_from_jsonb';
CREATE OR REPLACE FUNCTION zdbquery_to_json(zdbquery) RETURNS json PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_to_json';
CREATE OR REPLACE FUNCTION zdbquery_to_jsonb(zdbquery) RETURNS jsonb PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_to_jsonb';
CREATE CAST (text AS zdbquery) WITH FUNCTION zdbquery_from_text(text) AS IMPLICIT;
CREATE CAST (json AS zdbquery) WITH FUNCTION zdbquery_from_json(json) AS IMPLICIT;
CREATE CAST (jsonb AS zdbquery) WITH FUNCTION zdbquery_from_jsonb(jsonb) AS IMPLICIT;
CREATE CAST (zdbquery AS json) WITH FUNCTION zdbquery_to_json(zdbquery) AS IMPLICIT;
CREATE CAST (zdbquery AS jsonb) WITH FUNCTION zdbquery_to_jsonb(zdbquery) AS IMPLICIT;
CREATE OR REPLACE FUNCTION zdbquery(count_estimation integer, user_query text) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_ctor';
CREATE OR REPLACE FUNCTION zdbquery(count_estimation integer, user_query json) RETURNS zdbquery PARALLEL SAFE IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdbquery_ctor'; /* NB:  same C func as above */

--
-- query functions
--
CREATE OR REPLACE FUNCTION query(index regclass, query zdbquery) RETURNS SETOF tid IMMUTABLE STRICT ROWS 2500 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_query_srf';
CREATE OR REPLACE FUNCTION query_raw(index regclass, query zdbquery) RETURNS SETOF tid SET zdb.ignore_visibility = true IMMUTABLE STRICT ROWS 2500 LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_query_srf';
CREATE OR REPLACE FUNCTION query_tids(index regclass, query zdbquery) RETURNS tid[] IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_query_tids';
CREATE OR REPLACE FUNCTION profile_query(index regclass, query zdbquery) RETURNS json IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_profile_query';


