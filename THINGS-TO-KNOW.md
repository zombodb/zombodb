## Important Things To Know

### General Design Notes

Postgres\<-->Elasticsearch integrations are usually implemented in application code and are typically asynchronous,
meaning that Elasticsearch index updates appear to query results some time in the future. ZomboDB is not this kind of
integration.

ZomboDB ties into Postgres' Index Access Method API, which means it's synchronous. This is not to say that ZomboDB isn't
also concurrent -- it most definitely is. However, any COPY/INSERT/UPDATE/DELETE in a given Postgres session against a
table with a ZomboDB index will round-trip to Elasticsearch.

ZomboDB does, however, batch Elasticsearch indexing requests by transaction (**not** by row). This approach reduces the
number of round trips to Elasticsearch to the minimal amount. If during a transaction, a batch needs to be sent to
Elasticsearch in order to properly a search (a SELECT or aggregate function) then ZomboDB will do that automatically.

Because ZomboDB is an index type, it (along with help from Postgres) guarantees MVCC correctness across all queries that
use it. This includes normal WHERE clause conditions along with SQL-functions that perform Elasticsearch-specifc
aggregates that are wholly solved within the Elasticsearch cluster.

As such, any sort of failure either with ZomboDB itself, between Postgres and Elasticsearch (network layer), or within
Elasticsearch will cause the operating Postgres transaction to ABORT.

This is most definitely by design. Yet it's important to realize that such failures are pushed forward to the client.

### ZomboDB Defaults to **ZERO** Elasticsearch Index Replicas

ZomboDB's `VACUUM` implementation requires that, when it deletes dead docs from the backing Elasticsearch index, it must
`?wait_for_active_shards=all`. This is necessary to ensure that deleted docs are fully replicated before Postgres
decides to re-use those tuple slots.

In a typical development environment you're likely only going to have one Elasticsearch node (which is fine). So if you
had replicas set to >=1, ZomboDB's VACUUM process would hang (until timeout) waiting for the replicas to receive the
delete requests -- because with only one node, replicas are in an uninitialized state.

As such, ZomboDB provides a Postgres GUC called `zdb.default_replicas` (default is zero) that you can set on production
servers where you have a well-configured Elasticsearch cluster. You can also control the number of replicas per index
with the `replicas` index option.

### You Always Want Postgres to Plan an IndexScan

While ZomboDB works just fine with query plans that plan Sequential Scans, Bitmap Index Scans, and other scans with
Filter/Recheck conditions, you really want Postgres to choose an Index Scan against the ZomboDB index you intend to use.

ZomboDB assumes, by default, that the number of rows returned from a `==>` query will be 2500. For large tables, this is
a good default that generally convinces Postgres that an Index Scan is the right choice. You can, however, override this
number either via the [`zdb.default_row_estimate`](CONFIGURATION-SETTINGS.md#zdbdefault_row_estimate) GUC,
or per query (see [`dsl.row_estimate()`](QUERY-BUILDER-API.md#dslrow_estimate)).

So as usual with Postgres, if you're troubleshooting "slow queries", make sure to `EXPLAIN` your query and ensure it's
using an Index Scan.

It's also good to know that an Index Scan against a ZomboDB index returns the matching tuples in heap order (unlike a
standard Postgres btree index), so it's actually fairly efficient because it's effectively doing a sequential scan on
the heap (just likely skipping lots of pages along the way).

Note that if your query, however, requests scores (via `zdb.score()`) **or** has a `LIMIT` clause, then the tuples will
be returned in descending score order (highest-scoring document first). This is only true when an Index Scan is planned,
but can be a big performance boost because you won't also need to order the results by score.

### Indexing More Columns Means More Elasticsearch Abilities

ZomboDB is capable of anwering any Elasticsearch query, with correct MVCC results, wholly within Elasticsearch. This
means that complex aggregate queries can be answered in parallel across your Elasticsearch cluster, whereas the
corresponding SQL "group by" query may run in a single thread on your Postgres node.

So while there is a cost in terms of storage and indexing time to index many columns, it may make sense to do so in
order to maintain fast search response times.

### ZomboDB Stores the Entire Row in Elasticsearch

In order for ZomboDB to guarantee MVCC correct results it needs to track certain transaction visibility values in
Elasticsearch. Additionally, it needs to update those values when tuples are updated or deleted or vacuumed away. This
necessitates that ZomboDB store the entire document source for each indexed row so that Elasticsearch can properly
update the corresponding indexed document.

This means your Elasticsearch index sizes are most likely going to be measurably *larger* (perhaps close to 2x larger)
than the on-disk representation in Postgres. This is due to storing the document source plus all the indexed/analyzed
terms for every field. Keep this in mind when designing your Elasticsearch cluster.

It is worth noting that in extremely exceptional cases, in order to significantly reduce the size of the index, it may be useful to disable the `include_source` option (described [here](https://github.com/zombodb/zombodb/blob/master/INDEX-MANAGEMENT.md#include_source)).

### ZomboDB Rewrites Your Queries and CREATE INDEX Statements

When you write a ZomboDB query, you use the `==>` operator. The left-hand-side of `==>` is a reference to the table you
want to query, and the right-hand-side is your actual Elasticsearch query.

During the Postgres query planning phase, ZomboDB rewrites this so that the left-hand-side of `==>` is actually the
`ctid` system column of the table you originally specified.

For example, the query `SELECT * FROM table WHERE table ==> 'foo'` is rewritten as if you had actually specified
`SELECT * FROM table WHERE table.ctid ==> 'foo'`.

This is transparent to you, but is necessary in order for ZomboDB to support all of Postgres' various query plan types,
including plans that include sequential scans and hash joins.

### ZomboDB Attaches "hidden" Triggers to Tables

When you `CREATE INDEX ... ON table USING zombodb (...)` ZomboDB attaches two "hidden" triggers (they're considered
`tgisinternal` triggers) to the `table`. The triggers are FOR EACH ROW BEFORE UPDATE and DELETE triggers that track MVCC
visibility changes as part of the UPDATE and DELETE statement.

They're uniquely named and shouldn't cause conflicts with triggers you might need to add yourself.

Additionally, the triggers have a catalog dependency on the ZomboDB index, so when you drop the index, the triggers are
automatically dropped.

This should be a worry-free thing, but it's something to know.

### External Tools Like Kibana are Supported

Not only tools like Kibana, but you can search ZomboDB-managed indices with curl, if you want. But there's a catch...

ZomboDB stores dead rows, aborted rows, and in-flight rows from active transactions, and manages this state itself with
help from Postgres.

If you're searching the indices with an external tool, you're going to see those rows too because you don't also have
Postgres helping you.

It's also important to mention that you should **NOT** be modifying ZomboDB-managed indices with external tools. While
there's nothing stopping you from doing this you'll end up breaking ZomboDB's results from within Postgres, and that's
the whole reason you decided to install ZomboDB.
