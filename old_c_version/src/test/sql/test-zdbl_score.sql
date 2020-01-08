-- with no query... should return zero for all returned rows
SELECT zdb.score(ctid) FROM events LIMIT 10;

-- with a random tid value... should raise an ERROR
SELECT zdb.score('(1,1)'::tid);

SET enable_seqscan TO OFF;
SET enable_indexscan TO OFF;
SET enable_bitmapscan TO OFF;

-- with a seqscan
SET enable_seqscan TO ON;
EXPLAIN (COSTS OFF) SELECT zdb.score(ctid) > 0.0 FROM events WHERE events ==> 'beer';
SELECT zdb.score(ctid) > 0.0 FROM events WHERE events ==> 'beer';
SET enable_seqscan TO OFF;

-- with an indexscan
SET enable_indexscan TO ON;
EXPLAIN (COSTS OFF) SELECT zdb.score(ctid) > 0.0 FROM events WHERE events ==> 'beer';
SELECT zdb.score(ctid) > 0.0 FROM events WHERE events ==> 'beer';
SET enable_indexscan TO OFF;

-- with a bitmap index scan
SET enable_bitmapscan TO ON;
EXPLAIN (COSTS OFF) SELECT zdb.score(ctid) > 0.0 FROM events WHERE events ==> 'beer';
SELECT zdb.score(ctid) > 0.0 FROM events WHERE events ==> 'beer';
SET enable_bitmapscan TO OFF;
