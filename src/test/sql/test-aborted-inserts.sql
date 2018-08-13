CREATE TABLE aborted_inserts (
  id SERIAL8 NOT NULL PRIMARY KEY
);
CREATE INDEX idxaborted_inserts ON aborted_inserts USING zombodb ((aborted_inserts));

INSERT INTO aborted_inserts (id) VALUES (default);

BEGIN;
INSERT INTO aborted_inserts (id) VALUES (default);
ABORT;

VACUUM aborted_inserts;

SELECT
  (SELECT count(*) FROM aborted_inserts)                                                      AS aborted_inserts,
  (SELECT count(*) FROM aborted_inserts WHERE aborted_inserts ==> range(field=>'id', gte=>0)) AS aborted_inserts_all,
  (SELECT count(*) FROM aborted_inserts WHERE aborted_inserts ==> field_missing('id'))        AS aborted_inserts_null,
  (SELECT zdb.count('idxaborted_inserts', range(field=>'id', gte=>0)))                       AS aborted_inserts_estimate_all,
  (SELECT zdb.count('idxaborted_inserts', field_missing('id')))                              AS aborted_inserts_estimate_null;

DROP TABLE aborted_inserts CASCADE;