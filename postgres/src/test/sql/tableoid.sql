BEGIN;

CREATE TABLE test_tableoid (
  id SERIAL8 NOT NULL PRIMARY KEY,
  title text
);

CREATE INDEX idxtest_tableoid ON test_tableoid USING zombodb(zdb(tableoid, ctid), zdb(test_tableoid)) WITH (url='http://localhost:9200/');

INSERT INTO test_tableoid (title) VALUES ('title1');
INSERT INTO test_tableoid (title) VALUES ('title2');
INSERT INTO test_tableoid (title) VALUES ('title3');

SET enable_seqscan TO off;
EXPLAIN (costs off) SELECT * FROM test_tableoid WHERE zdb(tableoid, ctid) ==> 'title*';

SET enable_seqscan TO on;
SET enable_indexscan TO off;
EXPLAIN (costs off) SELECT * FROM test_tableoid WHERE zdb(tableoid, ctid) ==> 'title*';

ABORT;