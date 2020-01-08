SELECT count(*), count(*) = 126245, zdb.count('idxevents', match_all()), zdb.count('idxevents', match_all()) = 126245 FROM events;
ALTER TABLE events SET (autovacuum_enabled = false);
VACUUM events;

SELECT zdb.count('idxevents', match_all());
UPDATE events SET id = id WHERE id <= 1000;
SELECT zdb.count('idxevents', match_all());

SELECT zdb.count('idxevents', match_all());
BEGIN;
UPDATE events SET id = id WHERE id <= 1000;
SELECT zdb.count('idxevents', match_all());
ABORT;
SELECT zdb.count('idxevents', match_all());

ALTER TABLE events SET (autovacuum_enabled = true);

-- after a vacuum we should have no aborted xids
VACUUM events;
SELECT jsonb_array_length((zdb.request('idxevents', 'doc/zdb_aborted_xids?pretty')::jsonb)->'_source'->'zdb_aborted_xids');

-- MVCC count should match raw count
--   NB:  zdb.raw_count() always returns 1 more doc than we expect because of the 'zdb_aborted_xids' doc
SELECT zdb.count('idxevents', match_all()),
       zdb.raw_count('idxevents', match_all()),
       zdb.count('idxevents', match_all())+1 = zdb.raw_count('idxevents', match_all());

