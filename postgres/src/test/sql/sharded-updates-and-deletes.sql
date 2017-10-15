DROP TABLE IF EXISTS suad;

CREATE TABLE suad (
  id SERIAL8 NOT NULL PRIMARY KEY
) WITH (autovacuum_enabled = false);
CREATE INDEX idxsuad
  ON suad USING zombodb (zdb('suad', ctid), zdb(suad)) WITH (url='localhost:9200/', shards=1);

INSERT INTO suad (id) SELECT generate_series(1, 1);

BEGIN;
  UPDATE suad SET id = id;
  SELECT assert(zdb_estimate_count('suad', ''), 1, 'in commit');
COMMIT;

BEGIN;
  UPDATE suad SET id = id;
  SELECT assert(zdb_estimate_count('suad', ''), 1, 'in abort');
ABORT;
SELECT assert(zdb_estimate_count('suad', ''), 1, 'after abort');


-- BEGIN;
-- INSERT INTO suad (id) VALUES (default);
-- ABORT;
--
-- VACUUM suad;
--
-- SELECT
--   (SELECT count(*) FROM suad)                                           AS suad,
--   (SELECT count(*) FROM suad WHERE zdb('suad', ctid) ==> 'id:*')    AS suad_all,
--   (SELECT count(*) FROM suad WHERE zdb('suad', ctid) ==> 'id:null') AS suad_null,
--   (SELECT zdb_estimate_count('suad', 'id:*'))                       AS suad_estimate_all,
--   (SELECT zdb_estimate_count('suad', 'id:null'))                    AS suad_estimate_null;
--
-- DROP TABLE suad CASCADE;