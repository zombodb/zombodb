SET enable_seqscan TO OFF;
SET enable_indexscan TO OFF;
SET enable_bitmapscan TO OFF;

SET enable_indexscan TO ON;
EXPLAIN (COSTS OFF) SELECT id FROM events WHERE events ==> 'beer' LIMIT 10;
SELECT 1 FROM events WHERE events ==> 'beer' LIMIT 10;