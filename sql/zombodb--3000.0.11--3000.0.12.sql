DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.0.12 (ca389e2485d1cacbbd251161c2bda10fb5c55ffe)'
$$;
DROP FUNCTION IF EXISTS zdb.restrict(root internal, _operator_oid oid, args internal, var_relid int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.restrict(planner_info internal, _operator_oid oid, args internal, var_relid int4) RETURNS float8 AS 'MODULE_PATHNAME', 'restrict_wrapper' IMMUTABLE LANGUAGE c PARALLEL SAFE;

