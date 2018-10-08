# _cat API

ZomboDB exposes all of the Elasticsearch `_cat/` API endpoints as a set of typed views.  For the endpoints that include index names, only the indices that are managed by ZomboDB, from the current database, are included.

The cat API views also support ZomboDB indices from multiple Elasticsearch clusters (ie, indices have different `url` parameters).

The cat API views are extremely powerful as all the columns are properly typed and SQL aggregates can be used to perform more complex analysis and roll-ups of the raw cat API data.

## VIEW zdb.index_stats

This is strictly **not** part of the cat API, but is a simple view that provides a quick overview of all ZomboDB indices.

```sql
SELECT * FROM zdb.index_stats;
                        alias                        |                 index_name                 |          url           | table_name | es_docs | es_size | es_size_bytes | pg_docs_estimate | pg_size | pg_size_bytes | shards | replicas | doc_count | aborted_xids 
-----------------------------------------------------+--------------------------------------------+------------------------+------------+---------+---------+---------------+------------------+---------+---------------+--------+----------+-----------+--------------
 contrib_regression.public.users.idxusers-19626987   | 19612842.2200.19613374.19626987-511460435  | http://localhost:9200/ | users      | 264308  | 49 MB   |      51656478 |           264308 | 44 MB   |      45916160 | 5      | 0        | 264308    |            0
 contrib_regression.public.events.idxevents-19626984 | 19612842.2200.19613366.19626984-1471956215 | http://localhost:9200/ | events     | 415709  | 725 MB  |     760595414 |           117810 | 150 MB  |     157614080 | 5      | 0        | 126246    |            0
```

The `aborted_xids` column indicates the number of aborted transaction ids ZomboDB is tracking for each index.  (auto)VACUUM will decrease this number, eventually reaching zero when no concurrent modifications are occurring.

## VIEW zdb.cat_aliases

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-alias.html

Shows information about currently configured aliases to indices including filter and routing info.

---

## VIEW zdb.cat_allocation

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-allocation.html

Provides a snapshot of how many shards are allocated to each data node and how much disk space they are using.

---

## VIEW zdb.cat_count

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-count.html

Provides quick access to the document count of the entire cluster, or individual indices.

---

## VIEW zdb.cat_fielddata

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-fielddata.html

Shows how much heap memory is currently being used by fielddata on every data node in the cluster.

---

## VIEW zdb.cat_health

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-health.html

Shows various metrics regarding the health of each Elasticsearch cluster used by ZomboDB in the current database

---

## VIEW zdb.cat_indices

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-indices.html

Provides numerious metrics regarding the size of an index.  ZomboDB adds an `alias` column for a human-readable representation of each named index

---

## VIEW zdb.cat_master

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-master.html

Returns the master’s node ID, bound IP address, and node name.

---

## VIEW zdb.cat_nodeattrs

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-nodeattrs.html

Shows custom node attributes

---

## VIEW zdb.cat_nodes

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-nodes.html

Shows the cluster topology

---

## VIEW zdb.cat_pending_tasks

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-pending-tasks.html

Returns a list of any cluster-level changes (e.g. create index, update mapping, allocate or fail shard) which have not yet been executed.

---

## VIEW zdb.cat_plugins

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-plugins.html

Provides a view per node of running plugins.

---

## VIEW zdb.cat_thread_pool

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-thread-pool.html

Shows cluster wide thread pool statistics per node. By default the active, queue and rejected statistics are returned for all thread pools.

---

## VIEW zdb.cat_shards

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-shards.html

Detailed view of what nodes contain which shards. It will tell you if it’s a primary or replica, the number of docs, the bytes it takes on disk, and the node where it’s located.

ZomboDB adds an `alias` column for a human-readable representation for each index.

## VIEW zdb.cat_segments

https://www.elastic.co/guide/en/elasticsearch/reference/current/cat-segments.html

Provides low level information about the segments in the shards of an index. It provides information similar to the `cat_segments` view.