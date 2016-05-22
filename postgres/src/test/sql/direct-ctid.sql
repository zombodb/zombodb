CREATE TABLE direct_ctid (
    id serial8 primary key,
    title phrase
);

CREATE INDEX idxdirect_ctid ON direct_ctid USING zombodb(ctid, zdb(direct_ctid)) WITH (url='http://localhost:9200/');
INSERT INTO direct_ctid (title) VALUES ('this is a test');
INSERT INTO direct_ctid (title) VALUES ('so is this');
INSERT INTO direct_ctid (title) VALUES ('words words words');
INSERT INTO direct_ctid (title) VALUES ('stuff and things');

SET enable_bitmapscan to off;
SET enable_indexscan to off;
SET enable_seqscan to off;

SET enable_bitmapscan to on;
EXPLAIN (COSTS OFF, TIMING off) SELECT * FROM direct_ctid WHERE ctid ==> 'test,words,things';
SELECT * FROM direct_ctid WHERE ctid ==> 'test,words,things' ORDER BY id;
SET enable_bitmapscan to off;

SET enable_indexscan to on;
EXPLAIN (COSTS OFF, TIMING off) SELECT * FROM direct_ctid WHERE ctid ==> 'test,words,things';
SELECT * FROM direct_ctid WHERE ctid ==> 'test,words,things' ORDER BY id;
SET enable_indexscan to off;

SET enable_seqscan to on;
EXPLAIN (COSTS OFF, TIMING off) SELECT * FROM direct_ctid WHERE ctid ==> 'test,words,things';
SELECT * FROM direct_ctid WHERE ctid ==> 'test,words,things' ORDER BY id;
SET enable_seqscan to off;

DROP TABLE direct_ctid;

