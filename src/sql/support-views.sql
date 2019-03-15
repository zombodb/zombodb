--
-- a view to get quick stats about all indexes
--
CREATE OR REPLACE VIEW index_stats AS
  WITH stats AS (
      SELECT
        indrelid :: REGCLASS                                                                 table_name,
        indexrelid::regclass                                                                 ,
        zdb.index_name(indexrelid)                                                          index_name,
        zdb.index_url(indexrelid)                                                           url,
        zdb.request(indexrelid, '_stats', 'GET')::json                                      stats,
        zdb.request(indexrelid, '_settings', 'GET')::json                                   settings
      FROM pg_index, pg_class
      where pg_class.oid = pg_index.indexrelid and relam = (select oid from pg_am where amname = 'zombodb')
  )
  SELECT
    (select array_to_string(array_agg(alias), ',') from zdb.cat_aliases where index = index_name) as alias,
    index_name,
    url,
    table_name,
    stats -> '_all' -> 'primaries' -> 'docs' -> 'count'                                     AS es_docs,
    pg_size_pretty((stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8) AS es_size,
    (stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8                 AS es_size_bytes,
    (SELECT reltuples::int8 FROM pg_class WHERE oid = table_name)                           AS pg_docs_estimate,
    pg_size_pretty(pg_total_relation_size(table_name))                                      AS pg_size,
    pg_total_relation_size(table_name)                                                      AS pg_size_bytes,
    stats -> '_shards' -> 'total'                                                           AS shards,
    settings -> index_name -> 'settings' -> 'index' ->> 'number_of_replicas'                AS replicas,
    (zdb.request(indexrelid, '_doc/_count', 'GET') :: JSON) -> 'count'                      AS doc_count,
    coalesce(json_array_length((zdb.request(indexrelid, '_doc/zdb_aborted_xids', 'GET') :: JSON) -> '_source'->'zdb_aborted_xids'), 0) AS aborted_xids
  FROM stats;
