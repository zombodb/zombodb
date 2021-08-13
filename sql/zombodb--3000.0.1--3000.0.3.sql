DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.0.3 (c3b67d88486221dc77481176a50b91b43ecc09aa)'
$$;
DROP FUNCTION IF EXISTS zdb.terms(index regclass, field_name text, query zdbquery, size_limit pg_catalog.int4, order_by termsorderby) CASCADE;
CREATE OR REPLACE FUNCTION zdb.terms(index regclass, field_name text, query zdbquery, size_limit pg_catalog.int4 DEFAULT '2147483647', order_by termsorderby DEFAULT 'count') RETURNS TABLE(term text, doc_count pg_catalog.int8) immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'terms_wrapper';
DROP FUNCTION IF EXISTS zdb.tally(index regclass, field_name text, stem text, query zdbquery, size_limit pg_catalog.int4, order_by termsorderby, shard_size pg_catalog.int4, count_nulls bool) CASCADE;
CREATE OR REPLACE FUNCTION zdb.tally(index regclass, field_name text, stem text, query zdbquery, size_limit pg_catalog.int4 DEFAULT '2147483647', order_by termsorderby DEFAULT 'count', shard_size pg_catalog.int4 DEFAULT '2147483647', count_nulls bool DEFAULT 'true') RETURNS TABLE(term text, count pg_catalog.int8) immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'tally_not_nested_wrapper';
DROP FUNCTION IF EXISTS zdb.terms_array(index regclass, field_name text, query zdbquery, size_limit pg_catalog.int4, order_by termsorderby) CASCADE;
CREATE OR REPLACE FUNCTION zdb.terms_array(index regclass, field_name text, query zdbquery, size_limit pg_catalog.int4 DEFAULT '2147483647', order_by termsorderby DEFAULT 'count') RETURNS text[] immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'terms_array_agg_wrapper';
DROP FUNCTION IF EXISTS zdb.terms_two_level(index regclass, field_first text, field_second text, query zdbquery, order_by twoleveltermsorderby, size_limit pg_catalog.int4) CASCADE;
CREATE OR REPLACE FUNCTION zdb.terms_two_level(index regclass, field_first text, field_second text, query zdbquery, order_by twoleveltermsorderby DEFAULT 'count', size_limit pg_catalog.int4 DEFAULT '2147483647') RETURNS TABLE(term_one text, term_two text, doc_count pg_catalog.int8) immutable PARALLEL safe LANGUAGE c AS 'MODULE_PATHNAME', 'terms_two_level_wrapper';

