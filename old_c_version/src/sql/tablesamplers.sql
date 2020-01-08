--
-- table samplers
--
CREATE OR REPLACE FUNCTION sampler(internal) RETURNS tsm_handler IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_table_sampler';
CREATE OR REPLACE FUNCTION diversified_sampler(internal) RETURNS tsm_handler IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_diversified_table_sampler';
CREATE OR REPLACE FUNCTION query_sampler(internal) RETURNS tsm_handler IMMUTABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_query_table_sampler';

