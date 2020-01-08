SET enable_seqscan TO OFF;
SET enable_indexscan TO OFF;
SET enable_bitmapscan TO OFF;

-- with the order by here, we'll get an index scan on event's pkey column (id) with ES as a filter
SET enable_indexscan TO ON;
EXPLAIN (COSTS OFF) SELECT id FROM events WHERE events ==> 'beer' ORDER BY id LIMIT 10;
SELECT id FROM events WHERE events ==> 'beer' ORDER BY id LIMIT 10;