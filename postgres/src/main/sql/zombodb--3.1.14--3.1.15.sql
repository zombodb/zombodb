DROP VIEW zdb_index_stats;
DROP VIEW zdb_index_stats_fast;

CREATE VIEW zdb_index_stats AS
  WITH stats AS (
      SELECT
        indrelid :: REGCLASS AS                                                                    table_name,
        indexrelid::regclass,
        zdb_get_index_name(indexrelid)                                                             index_name,
        zdb_get_url(indexrelid)                                                                    url,
        zdb_es_direct_request(indexrelid, 'GET', '_stats')::json                                  stats,
        zdb_es_direct_request(indexrelid, 'GET', '_settings')::json                               settings
      FROM pg_index
      WHERE pg_get_indexdef(indexrelid) ILIKE '%zombodb%'
  )
  SELECT
    index_name,
    url,
    table_name,
    stats -> '_all' -> 'primaries' -> 'docs' -> 'count'                                     AS es_docs,
    pg_size_pretty((stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8) AS es_size,
    (stats -> '_all' -> 'primaries' -> 'store' ->> 'size_in_bytes') :: INT8                 AS es_size_bytes,
    count_of_table(table_name)                                                              AS pg_docs,
    pg_size_pretty(pg_total_relation_size(table_name))                                      AS pg_size,
    pg_total_relation_size(table_name)                                                      AS pg_size_bytes,
    stats -> '_shards' -> 'total'                                                           AS shards,
    settings -> index_name -> 'settings' -> 'index' ->> 'number_of_replicas'                AS replicas,
    (zdb_es_direct_request(indexrelid, 'GET', 'data/_count')::json) -> 'count'             AS data_count,
    (zdb_es_direct_request(indexrelid, 'GET', 'state/_count')::json) -> 'count'            AS state_count,
    (zdb_es_direct_request(indexrelid, 'GET', 'committed/_count')::json) -> 'count'        AS xid_count,
    (zdb_es_direct_request(indexrelid, 'GET', 'deleted/_count')::json) -> 'count'          AS deleted_count
  FROM stats;

CREATE VIEW zdb_index_stats_fast AS
  WITH stats AS (
      SELECT
        indrelid :: REGCLASS AS                                                                    table_name,
        indexrelid::regclass,
        zdb_get_index_name(indexrelid)                                                             index_name,
        zdb_get_url(indexrelid)                                                                    url,
        zdb_es_direct_request(indexrelid, 'GET', '_stats')::json                                  stats,
        zdb_es_direct_request(indexrelid, 'GET', '_settings')::json                               settings
      FROM pg_index
      WHERE pg_get_indexdef(indexrelid) ILIKE '%zombodb%'
  )
  SELECT
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
    (zdb_es_direct_request(indexrelid, 'GET', 'data/_count')::json) -> 'count'             AS data_count,
    (zdb_es_direct_request(indexrelid, 'GET', 'state/_count')::json) -> 'count'            AS state_count,
    (zdb_es_direct_request(indexrelid, 'GET', 'committed/_count')::json) -> 'count'        AS xid_count,
    (zdb_es_direct_request(indexrelid, 'GET', 'deleted/_count')::json) -> 'count'          AS deleted_count
  FROM stats;
