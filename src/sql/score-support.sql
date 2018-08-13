--
-- scoring support
--
CREATE OR REPLACE FUNCTION score(ctid tid) RETURNS float4 PARALLEL SAFE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_score';

