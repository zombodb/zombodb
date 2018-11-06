--
-- scoring support
--
CREATE OR REPLACE FUNCTION score(ctid tid) RETURNS float4 PARALLEL UNSAFE LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_score';

