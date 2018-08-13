--
-- _cat/ API support
--
CREATE OR REPLACE FUNCTION cat_request(endpoint TEXT) RETURNS TABLE(url TEXT, response JSONB) PARALLEL SAFE IMMUTABLE STRICT LANGUAGE SQL AS $$
    WITH clusters AS (SELECT
                        zdb.index_url(idx) url,
                        idx
                      FROM (SELECT DISTINCT ON (zdb.index_url(oid::regclass)) oid::regclass idx
                            FROM pg_class
                            WHERE relam = (SELECT oid
                                           FROM pg_am
                                           WHERE amname = 'zombodb')) x)
    SELECT
      url,
      jsonb_array_elements(zdb.request(idx, '/_cat/' || $1 || '?h=*&format=json&time=ms&bytes=b&size=k')::jsonb)
    FROM clusters
$$;

CREATE OR REPLACE VIEW cat_aliases AS
    SELECT
        url,
        response->>'alias' AS "alias",
        response->>'index' AS "index",
        response->>'filter' AS "filter",
        response->>'routing.index' AS "routing.index",
        response->>'routing.search' AS "routing.search"
     FROM zdb.cat_request('aliases')
    WHERE response->>'index' IN (SELECT index from zdb.all_es_index_names() index);

CREATE OR REPLACE VIEW cat_allocation AS
    SELECT
        url,
        (response->>'shards')::int AS "shards",
        (response->>'disk.indices')::bigint AS "disk.indices",
        (response->>'disk.used')::bigint AS "disk.used",
        (response->>'disk.avail')::bigint AS "disk.avail",
        (response->>'disk.total')::bigint AS "disk.total",
        (response->>'disk.percent')::real AS "disk.percent",
        (response->>'host') AS "host",
        (response->>'ip')::inet AS "ip",
        (response->>'node') AS "node"
     FROM zdb.cat_request('allocation');

CREATE OR REPLACE VIEW cat_count AS
    SELECT
        url,
        (response->>'epoch')::bigint AS "epoch",
        (response->>'timestamp')::time AS "timestamp",
        (response->>'count')::bigint AS "count"
     FROM zdb.cat_request('count');

CREATE OR REPLACE VIEW cat_fielddata AS
    SELECT
        url,
        (response->>'id') AS "id",
        (response->>'host') AS "host",
        (response->>'ip')::inet AS "ip",
        (response->>'node') AS "node",
        (response->>'field') AS "field",
        (response->>'size')::bigint AS "size"
     FROM zdb.cat_request('fielddata');

CREATE OR REPLACE VIEW cat_health AS
    SELECT
        url,
        (response->>'epoch')::bigint AS "epoch",
        (response->>'timestamp')::time AS "timestamp",
        (response->>'cluster')::text AS "cluster",
        (response->>'status') AS "status",
        (response->>'node.total')::bigint AS "node.total",
        (response->>'node.data')::bigint AS "node.data",
        (response->>'shards')::int AS "shards",
        (response->>'pri')::int AS "pri",
        (response->>'relo')::int AS "relo",
        (response->>'init')::int AS "init",
        (response->>'unassign')::int AS "unassign",
        (response->>'pending_tasks')::int AS "pending_tasks",
        case when response->>'max_task_wait_time' <> '-' then ((response->>'max_task_wait_time')||'milliseconds')::interval else null end AS "max_task_wait_time",
        (response->>'active_shards_percent') AS "active_shards_percent"
     FROM zdb.cat_request('health');

CREATE OR REPLACE VIEW cat_indices AS
    SELECT
        url,
        (response->>'health') AS "health",
        (response->>'status') AS "status",
        (select array_to_string(array_agg(alias), ',') from zdb.cat_aliases where index = response->>'index') as alias,
        (response->>'index') AS "index",
        (response->>'uuid') AS "uuid",
        (response->>'pri')::int AS "pri",
        (response->>'rep')::int AS "rep",
        (response->>'docs.count')::bigint AS "docs.count",
        (response->>'docs.deleted')::bigint AS "docs.deleted",
        (response->>'store.size')::bigint AS "store.size",
        (response->>'pri.store.size')::bigint AS "pri.store.size"
     FROM zdb.cat_request('indices')
     WHERE response->>'index' IN (SELECT index from zdb.all_es_index_names() index);

CREATE OR REPLACE VIEW cat_master AS
    SELECT
        url,
        (response->>'id') AS "id",
        (response->>'host') AS "host",
        (response->>'ip')::inet AS "ip",
        (response->>'node') AS "node"
     FROM zdb.cat_request('master');

CREATE OR REPLACE VIEW cat_nodeattrs AS
    SELECT
        url,
        (response->>'node') AS "node",
        (response->>'host') AS "host",
        (response->>'ip')::inet AS "ip",
        (response->>'attr') AS "attr",
        (response->>'value') AS "value"
     FROM zdb.cat_request('nodeattrs');

CREATE OR REPLACE VIEW cat_nodes AS
    SELECT
        url,
        (response->>'id')::text AS "id",
        (response->>'pid')::int AS "pid",
        (response->>'ip')::inet AS "ip",
        (response->>'port')::int AS "port",
        (response->>'http_address')::text AS "http_address",
        (response->>'version')::text AS "version",
        (response->>'build')::text AS "build",
        (response->>'jdk')::text AS "jdk",

        (response->>'disk.total')::text AS "disk.total",
        (response->>'disk.used')::text AS "disk.used",
        (response->>'disk.avail')::text AS "disk.avail",
        (response->>'disk.used_percent')::real AS "disk.used_percent",

        (response->>'heap.current')::text AS "heap.current",
        (response->>'heap.percent')::real AS "heap.percent",
        (response->>'heap.max')::text AS "heap.max",

        (response->>'ram.current')::text AS "ram.current",
        (response->>'ram.percent')::text AS "ram.percent",
        (response->>'ram.max')::text AS "ram.max",

        (response->>'file_desc.current')::int AS "file_desc.current",
        (response->>'file_desc.percent')::real AS "file_desc.percent",
        (response->>'file_desc.max')::int AS "file_desc.max",

        (response->>'cpu')::real AS "cpu",
        (response->>'load.1m')::real AS "load.1m",
        (response->>'load.5m')::real AS "load.5m",
        (response->>'load.15m')::real AS "load.15m",
        (response->>'uptime')::text AS "uptime",

        (response->>'node.role')::text AS "node.role",
        (response->>'master') = '*' AS "master",
        (response->>'name')::text AS "name",
        (response->>'completion.size')::text AS "completion.size",

        (response->>'fielddata.memory_size')::text AS "fielddata.memory_size",
        (response->>'fielddata.evictions')::int AS "fielddata.evictions",

        (response->>'query_cache.memory.size')::text AS "query_cache.memory.size",
        (response->>'query_cache.evictions')::int AS "query_cache.evictions",

        (response->>'request_cache.memory_size')::text AS "request_cache.memory_size",
        (response->>'request_cache.evictions')::int AS "request_cache.evictions",
        (response->>'request_cache.hit_count')::int AS "request_cache.hit_count",
        (response->>'request_cache.miss_count')::int AS "request_cache.miss_count",

        (response->>'flush.total')::int AS "flush.total",
        ((response->>'flush.total_time')::text||' milliseconds')::interval AS "flush.total_time",

        (response->>'get.current')::int AS "get.current",
        ((response->>'get.time')::text::text||' milliseconds')::interval AS "get.time",
        (response->>'get.total')::bigint AS "get.total",
        ((response->>'get.exists_time')::text||' milliseconds')::interval AS "get.exists_time",
        (response->>'get.exists_total')::bigint AS "get.exists_total",
        ((response->>'get.missing_time')::text||' milliseconds')::interval AS "get.missing_time",
        (response->>'get.missing_total')::bigint AS "get.missing_total",

        (response->>'indexing.delete_current')::bigint AS "indexing.delete_current",
        ((response->>'indexing.delete_time')::text||' milliseconds')::interval AS "indexing.delete_time",
        (response->>'indexing.delete_total')::bigint AS "indexing.delete_total",
        (response->>'indexing.index_current')::bigint AS "indexing.index_current",
        ((response->>'indexing.index_time')::text||' milliseconds')::interval AS "indexing.index_time",
        (response->>'indexing.index_total')::bigint AS "indexing.index_total",
        (response->>'indexing.index_failed')::bigint AS "indexing.index_failed",

        (response->>'merges.current')::bigint AS "merges.current",
        (response->>'merges.current.docs')::bigint AS "merges.current.docs",
        (response->>'merges.current.size')::text AS "merges.current.size",
        (response->>'merges.total')::bigint AS "merges.total",
        (response->>'merges.total_docs')::bigint AS "merges.total_docs",
        (response->>'merges.total_size')::text AS "merges.total_size",
        ((response->>'merges.total_time')::text||' milliseconds')::interval AS "merges.total_time",

        (response->>'refresh.total')::bigint AS "refresh.total",
        ((response->>'refresh.time')::text::text||' milliseconds')::interval AS "refresh.time",
        (response->>'refresh.listeners')::text AS "refresh.listeners",

        (response->>'script.compilations')::bigint AS "script.compilations",
        (response->>'script.cache_evictions')::bigint AS "script.cache_evictions",

        (response->>'search.fetch_current')::bigint AS "search.fetch_current",
        ((response->>'search.fetch_time')::text||' milliseconds')::interval AS "search.fetch_time",
        (response->>'search.fetch_total')::bigint AS "search.fetch_total",
        (response->>'search.open_contexts')::bigint AS "search.open_contexts",
        (response->>'search.query_current')::bigint AS "search.query_current",
        ((response->>'search.query_time')::text||' milliseconds')::interval AS "search.query_time",
        (response->>'search.query_total')::text AS "search.query_total",

        (response->>'search.scroll_current')::bigint AS "search.scroll_current",
        ((response->>'search.scroll_time')::bigint/1000 ||' milliseconds')::interval AS "search.scroll_time",
        (response->>'search.scroll_total')::bigint AS "search.scroll_total",

        (response->>'segments.count')::bigint AS "segments.count",
        (response->>'segments.memory')::text AS "segments.memory",
        (response->>'segments.index_writer_memory')::text AS "segments.index_writer_memory",
        (response->>'segments.version_map_memory')::text AS "segments.version_map_memory",
        (response->>'segments.fixed_bitset_memory')::text AS "segments.fixed_bitset_memory",

        (response->>'suggest.current')::bigint AS "suggest.current",
        ((response->>'suggest.time')::text||' milliseconds')::interval AS "suggest.time",
        (response->>'suggest.total')::bigint AS "suggest.total"
     FROM zdb.cat_request('nodes');

CREATE OR REPLACE VIEW cat_pending_tasks AS
    SELECT
        url,
        (response->>'insertOrder')::int AS "insertOrder",
        ((response->>'timeInQueue')||' milliseconds')::interval AS "timeInQueue",
        (response->>'priority') AS "priority",
        (response->>'source') AS "source"
     FROM zdb.cat_request('pending_tasks');

CREATE OR REPLACE VIEW cat_plugins AS
    SELECT
        url,
        (response->>'name') AS "name",
        (response->>'component') AS "component",
        (response->>'version') AS "version",
        (response->>'description') AS "description"
     FROM zdb.cat_request('plugins');

CREATE OR REPLACE VIEW cat_thread_pool AS
    SELECT
        url,
        (response->>'ip')::inet AS "ip",
        (response->>'max')::int AS "max",
        (response->>'min')::int AS "min",
        (response->>'pid')::int AS "pid",
        (response->>'host')::text AS "host",
        (response->>'name')::text AS "name",
        (response->>'port')::int AS "port",
        (response->>'size')::int AS "size",
        (response->>'type')::text AS "type",
        (response->>'queue')::int AS "queue",
        (response->>'active')::int AS "active",
        (response->>'largest')::int AS "largest",
        (response->>'node_id')::text AS "node_id",
        (response->>'rejected')::int AS "rejected",
        (response->>'completed')::bigint AS "completed",
        (response->>'node_name')::text AS "node_name",
        (response->>'keep_alive')::text AS "keep_alive",
        (response->>'queue_size')::int AS "queue_size",
        (response->>'ephemeral_node_id')::text AS "ephemeral_node_id"
     FROM zdb.cat_request('thread_pool');

CREATE OR REPLACE VIEW cat_shards AS
    SELECT
        url,
        (response->>'id')::text AS "id",
        (response->>'ip')::inet AS "ip",
        (response->>'docs')::bigint AS "docs",
        (response->>'node')::text AS "node",
        (select array_to_string(array_agg(alias), ',') from zdb.cat_aliases where index = response->>'index') as alias,
        (response->>'index')::text AS "index",
        (response->>'shard')::int AS "shard",
        (response->>'state')::text AS "state",
        (response->>'store')::bigint AS "store",
        (response->>'prirep')::text AS "prirep",
        (response->>'sync_id')::text AS "sync_id",
        (response->>'completion.size')::bigint AS "completion.size",
        (response->>'fielddata.evictions')::int AS "fielddata.evictions",
        (response->>'fielddata.memory_size')::bigint AS "fielddata.memory_size",
        (response->>'flush.total')::bigint AS "flush.total",
        ((response->>'flush.total_time')||'milliseconds')::interval AS "flush.total_time",
        (response->>'get.current')::int AS "get.current",
        ((response->>'get.exists_time')||'milliseconds')::interval AS "get.exists_time",
        (response->>'get.exists_total')::bigint AS "get.exists_total",
        ((response->>'get.missing_time')||'milliseconds')::interval AS "get.missing_time",
        (response->>'get.missing_total')::bigint AS "get.missing_total",
        ((response->>'get.time')||'milliseconds')::interval AS "get.time",
        (response->>'get.total')::bigint AS "get.total",
        (response->>'indexing.delete_current')::int AS "indexing.delete_current",
        ((response->>'indexing.delete_time')||'milliseconds')::interval AS "indexing.delete_time",
        (response->>'indexing.delete_total')::bigint AS "indexing.delete_total",
        (response->>'indexing.index_current')::int AS "indexing.index_current",
        (response->>'indexing.index_failed')::bigint AS "indexing.index_failed",
        ((response->>'indexing.index_time')||'milliseconds')::interval AS "indexing.index_time",
        (response->>'indexing.index_total')::bigint AS "indexing.index_total",
        (response->>'merges.current')::int AS "merges.current",
        (response->>'merges.current_docs')::int AS "merges.current_docs",
        (response->>'merges.current_size')::bigint AS "merges.current_size",
        (response->>'merges.total')::bigint AS "merges.total",
        (response->>'merges.total_docs')::bigint AS "merges.total_docs",
        (response->>'merges.total_size')::bigint AS "merges.total_size",
        ((response->>'merges.total_time')||'milliseconds')::interval AS "merges.total_time",
        (response->>'query_cache.evictions')::int AS "query_cache.evictions",
        (response->>'query_cache.memory_size')::bigint AS "query_cache.memory_size",
        (response->>'recoverysource.type')::text AS "recoverysource.type",
        (response->>'refresh.listeners')::int AS "refresh.listeners",
        ((response->>'refresh.time')||'milliseconds')::interval AS "refresh.time",
        (response->>'refresh.total')::bigint AS "refresh.total",
        (response->>'search.fetch_current')::int AS "search.fetch_current",
        ((response->>'search.fetch_time')||'milliseconds')::interval AS "search.fetch_time",
        (response->>'search.fetch_total')::bigint AS "search.fetch_total",
        (response->>'search.open_contexts')::int AS "search.open_contexts",
        (response->>'search.query_current')::int AS "search.query_current",
        ((response->>'search.query_time')||'milliseconds')::interval AS "search.query_time",
        (response->>'search.query_total')::bigint AS "search.query_total",
        (response->>'search.scroll_current')::int AS "search.scroll_current",
        ((response->>'search.scroll_time')::bigint / 1000 ||'milliseconds')::interval AS "search.scroll_time",
        (response->>'search.scroll_total')::bigint AS "search.scroll_total",
        (response->>'segments.count')::int AS "segments.count",
        (response->>'segments.fixed_bitset_memory')::bigint AS "segments.fixed_bitset_memory",
        (response->>'segments.index_writer_memory')::bigint AS "segments.index_writer_memory",
        (response->>'segments.memory')::bigint AS "segments.memory",
        (response->>'segments.version_map_memory')::bigint AS "segments.version_map_memory",
        (response->>'unassigned.at')::text AS "unassigned.at",
        (response->>'unassigned.details')::text AS "unassigned.details",
        (response->>'unassigned.for')::text AS "unassigned.for",
        (response->>'unassigned.reason')::text AS "unassigned.reason",
        (response->>'warmer.current')::int AS "warmer.current",
        (response->>'warmer.total')::bigint AS "warmer.total",
        ((response->>'warmer.total_time')||'milliseconds')::interval AS "warmer.total_time"
     FROM zdb.cat_request('shards')
     WHERE response->>'index' IN (SELECT index from zdb.all_es_index_names() index);

CREATE OR REPLACE VIEW cat_segments AS
    SELECT
        url,
        (response->>'id')::text AS "id",
        (response->>'ip')::inet AS "ip",
        (response->>'size')::bigint AS "size",
        (select array_to_string(array_agg(alias), ',') from zdb.cat_aliases where index = response->>'index') as alias,
        (response->>'index')::text AS "index",
        (response->>'shard')::int AS "shard",
        (response->>'prirep')::text AS "prirep",
        (response->>'segment')::text AS "segment",
        (response->>'version')::text AS "version",
        (response->>'compound')::boolean AS "compound",
        (response->>'committed')::boolean AS "committed",
        (response->>'docs.count')::bigint AS "docs.count",
        (response->>'generation')::int AS "generation",
        (response->>'searchable')::boolean AS "searchable",
        (response->>'size.memory')::bigint AS "size.memory",
        (response->>'docs.deleted')::bigint AS "docs.deleted"
     FROM zdb.cat_request('segments')
     WHERE response->>'index' IN (SELECT index from zdb.all_es_index_names() index);

