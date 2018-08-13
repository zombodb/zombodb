SELECT zdb.index_name('idxevents') IS NOT NULL; -- b/c the index name is a uuid and different every time
SELECT zdb.index_url('idxevents') IS NOT NULL; -- b/c we might have a different default url set
SELECT id FROM events WHERE ctid IN (SELECT * FROM zdb.query('idxevents', 'beer')) ORDER BY id;
SELECT id FROM events WHERE ctid IN (SELECT unnest(zdb.query_tids('idxevents', 'beer'))) ORDER BY id;
SELECT zdb.to_query_dsl('beer');
SELECT zdb.to_queries_dsl(ARRAY['beer','wine','cheese']);
