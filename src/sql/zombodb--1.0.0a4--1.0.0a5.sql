DROP VIEW cat_indices;
DROP VIEW cat_shards;
DROP VIEW cat_segments;

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

