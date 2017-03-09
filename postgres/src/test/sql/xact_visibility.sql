BEGIN;
CREATE TABLE xact_test (
  id serial8 not null primary key,
  title text
);
CREATE INDEX idxxact_test ON xact_test USING zombodb(zdb(xact_test), zdb_to_json(xact_test)) WITH (url='http://localhost:9200/');

INSERT INTO xact_test (title) values ('test 1');
INSERT INTO xact_test (title) values ('test 2');
INSERT INTO xact_test (title) values ('test 3');

SELECT * FROM xact_test WHERE zdb(xact_test) ==> 'title:test*' ORDER BY id;

UPDATE xact_test SET title = 'foo' WHERE title = 'test 1';
SELECT * FROM xact_test WHERE zdb(xact_test) ==> '' ORDER BY id;

SELECT * FROM zdb_tally('xact_test', 'title', '^.*', '', 5000, 'term');
ABORT;