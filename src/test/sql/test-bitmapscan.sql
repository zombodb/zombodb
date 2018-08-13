SET enable_seqscan TO OFF;
SET enable_indexscan TO OFF;
SET enable_bitmapscan TO OFF;

SET enable_bitmapscan TO ON;
EXPLAIN (COSTS OFF) SELECT id FROM events WHERE events ==> 'beer' ORDER BY id LIMIT 10;
SELECT id FROM events WHERE events ==> 'beer' ORDER BY id LIMIT 10;