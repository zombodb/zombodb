SELECT count(*), count(*) = 126245, zdb.count('idxevents', match_all()), zdb.count('idxevents', match_all()) = 126245 FROM events;
ALTER TABLE events SET (autovacuum_enabled = false);
VACUUM events;


BEGIN;
SELECT zdb.count('idxevents', match_all());
INSERT INTO events (id) SELECT generate_series(1000000, 1001000);
SELECT zdb.count('idxevents', match_all());
ABORT;

SELECT zdb.count('idxevents', match_all());
INSERT INTO events (id) SELECT generate_series(1000000, 1001000);
SELECT zdb.count('idxevents', match_all());

SELECT zdb.count('idxevents', match_all());
DELETE FROM events WHERE id >= 1000000;
SELECT zdb.count('idxevents', match_all());
VACUUM events;


-- MVCC count should match raw count
--   NB:  zdb.raw_count() always returns 1 more doc than we expect because of the 'zdb.aborted_xids' doc
SELECT zdb.count('idxevents', match_all()),
       zdb.raw_count('idxevents', match_all()),
       zdb.count('idxevents', match_all())+1 = zdb.raw_count('idxevents', match_all());

ALTER TABLE events SET (autovacuum_enabled = true);
