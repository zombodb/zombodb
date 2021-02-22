ALTER INDEX idxevents SET (replicas = 1, shards=32);
SELECT replicas FROM zdb.index_stats WHERE es_index_name = zdb.index_name('idxevents');
ALTER INDEX idxevents SET (replicas = 0, shards=5);
SELECT replicas FROM zdb.index_stats WHERE es_index_name = zdb.index_name('idxevents');

ALTER INDEX idxevents SET (type_name = 'cant_chage_this');
ALTER INDEX idxevents SET (uuid = 'foo');

ALTER TABLE events ADD COLUMN foo2 text;
SELECT (zdb.index_mapping('idxevents')->zdb.index_name('idxevents')->'mappings'->'properties'->'foo2')::jsonb;
ALTER TABLE events DROP COLUMN foo2;

ALTER INDEX idxevents RESET (alias);     SELECT substring(alias, 1, strpos(alias, '-')) FROM zdb.cat_aliases WHERE index = zdb.index_name('idxevents');
ALTER INDEX idxevents SET (alias='foo'); SELECT substring(alias, 1, strpos(alias, '-')) FROM zdb.cat_aliases WHERE index = zdb.index_name('idxevents');
ALTER INDEX idxevents RESET (alias);     SELECT substring(alias, 1, strpos(alias, '-')) FROM zdb.cat_aliases WHERE index = zdb.index_name('idxevents');
