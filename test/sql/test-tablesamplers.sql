SELECT count(*) FROM events TABLESAMPLE zdb.query_sampler('idxevents', 'beer');
SELECT count(*) FROM events TABLESAMPLE zdb.sampler('idxevents', 100, '');
SELECT count(*) FROM events TABLESAMPLE zdb.diversified_sampler('idxevents', 100, 'id', '');
