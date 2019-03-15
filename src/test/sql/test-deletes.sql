SELECT count(*), count(*) = 126245, zdb.count('idxevents', match_all()), zdb.count('idxevents', match_all()) = 126245 FROM events;
ALTER TABLE events SET (autovacuum_enabled = false);
VACUUM events;

-- save the data we're about to delete so we can INSERT it back when we're done
CREATE TABLE delete_tmp AS SELECT * FROM events WHERE id <= 1000;

-- delete those rows, but then abort the DELETE
SELECT zdb.count('idxevents', match_all());
BEGIN;
DELETE FROM events WHERE id IN (SELECT id FROM delete_tmp);
SELECT zdb.count('idxevents', match_all());
ABORT;
SELECT zdb.count('idxevents', match_all());

-- delete them again, but this time commit the DELETE
SELECT zdb.count('idxevents', match_all());
DELETE FROM events WHERE id IN (SELECT id FROM delete_tmp);
SELECT zdb.count('idxevents', match_all());

-- restore the rows
SELECT zdb.count('idxevents', match_all());
INSERT INTO events SELECT * FROM delete_tmp;
SELECT zdb.count('idxevents', match_all());
VACUUM events;

-- shouldn't have any aborted xids now
SELECT (zdb.request('idxevents', '_doc/zdb.aborted_xids?pretty')::jsonb)->'_source'->'zdb.aborted_xids';

-- MVCC count should match raw count
--   NB:  zdb.raw_count() always returns 1 more doc than we expect because of the 'zdb.aborted_xids' doc
SELECT zdb.count('idxevents', match_all()),
       zdb.raw_count('idxevents', match_all()),
       zdb.count('idxevents', match_all())+1 = zdb.raw_count('idxevents', match_all());

ALTER TABLE events SET (autovacuum_enabled = true);

DROP TABLE delete_tmp CASCADE;


