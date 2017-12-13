# Block Routing

Block Routing is an approach to optimizing [cross-index joins](CROSS-INDEX-JOINS.md) by controlling the Elasticsearch shard documents are routed to during indexing.

In short, if ZomboDB knows which shard contains a block of primary key values it can limit its cross-index join queries to that shard, reducing the amount of total queries that need to be executed.

## Default Document Routing

By default, ZomboDB allows Elasticsearch to determine document routing using the `_id` field of each doc.  The `_id` value is a textual representation of the Postgres "tuple id" (hidden system column named `ctid`) for each row.

In general, this provides a fairly evenly-distributed routing value that allows your shards to be of nearly equal size in terms of document count (and likely byte count too).

Technically, ZomboDB forcefully sets the doc's `_routing` value to `_id`, but that's effectively no different than if Elasticsearch was making its default decision to use `_id` anyways.

## Routing with Block Routing

(If your tables have less than 100,000 rows, you can probably just stop reading now.)

To enable block routing, you need to specificy a column name to use during `CREATE INDEX` using the option `block_routing_field`.  This column must be an integer type such as `int2 (smallint)`, `int4 (integer)`, or `int8 (bigint)`.

Behind the scenes, when indexing data, ZomboDB will extract the value of this column and round it to its closest matching block of 100,000*, and use that value for `_routing`.

This means, for example, that documents with a value in the range of `[0..99,999]` will route with a value of `0`, documents in the range `[100,000..199,999]` will route with a value of `1`, etc, etc.  The formula is simply:

```java
    long block = (long) (val / 100_000L)
```

Elasticsearch then takes that block number (which is set as the `_routing`), applies its normal shard hashing algorithm and places the document on the correct shard.

###### *currently, the block size is hardcoded to 100,000 and is not configurable.

## When is Block Routing Useful?

When you have multiple tables with potentially millions of rows and you typically execute queries that join across those tables.

## How to Enable Block Routing?

You enable block routing by setting the `block_routing_field` to the column name of an integer column (typically the table's primary key or other "join key") on each `USING zombodb` index that will be involved in joins.

It's also required that the number of `shards` be the same across all indexes that you will be using with Block Routing, otherwise ZomboDB will not be able to perform its optimizations at query time.

## How does it Work?

Lets say you have a simple schema similar to this:

```sql
CREATE TABLE book (id serial4 NOT NULL PRIMARY KEY, title phrase, author text);
CREATE TABLE book_content (book_id int4 NOT NULL, body fulltext, pageno int4);
CREATE INDEX idxbook ON book USING zombodb (zdb('book', ctid), zdb(book)) 
        WITH (url='...', shards=40, block_routing_field='id', options='id=<book_content.idxcontent>book_id');
CREATE INDEX idxcontent on book_content USING zombodb (zdb('book_content', ctid), zdb(book_content)) 
        WITH (url='...', shards=40, block_routing_field='book_id');
```

Note that the index on `book` has a `block_routing_field` value of `id`, the index on `book_content` has a `block_routing_field` value of `book_id`, and both indexes have identical `shards` values.  Also note that index linking `options` on `idxbook` are set to enable joining to `book_content` using `book.id = book_content.book_id`.

If you were to execute a query such as `SELECT * FROM book WHERE zdb('book', ctid) ==> 'body:beer';`, ZomboDB will generate an Elasticsearch query similar to:

```json
       "cross_join" : {                              
         "cluster_name" : "zdb-test",                
         "host" : "192.168.0.99",                            
         "port" : 9300,                              
         "index" : "tutorial.public.book_content.idxcontent",
         "type" : "data",                            
         "left_fieldname" : "id",                    
         "right_fieldname" : "book_id",                
         "can_optimize_joins" : true,                
         "query" : {                                 
           "term" : {                                
             "body" : {                          
               "value" : "beer",                     
               "boost" : 1.0                         
             }                                       
           }                                         
        }
```

As explained in [CROSS-INDEX-JOINS.md](CROSS-INDEX-JOINS.md), ZomboDB will run a CrossJoinQuery on every shard for `book` that then uses the FastTerms shard action to query **every** shard in `book_content`, and then rewrite the final query to find `book` documents with an `id` contained in the set of all `book_id`s found by FastTerms.  And again, all of this is happening for every shard in `books` (40, in this example).

Note here that the `can_optimize_joins` proeprty is `true` in the generated query.  This means ZomboDB examined both `idxbook` and `idxcontent` and determined that they're routed in a way that's equivalent.  As such, the block routing optimization can kick in.

What happens is that when the CrossJoinQuery is executing on, say, shard 2 of `idxbook`, it only needs to use FastTerms to query shard 2 on `idxcontent` because there's no possible way any of the other shards in `idxcontent` could contain docs with `book_content.book_id` values that would match the `book.id` values contained on shard 2 of `book`.  This is because we defined each index to use block routing on the fields that were defined as equivalent in the index linking `options`.

As such, the combinatorial query explosion is defeated, and we now have a 1-to-1 mapping of join key values between physical index shards.

Not only does this reduce the number of parallel queries that need to be executed, ZomboDB may be able to reduce the docs found on each shard into a small set of range queries (depending on how closely packed by `block_routing_field` the matching docs are) which ultimately executes very quickly.

## Limitations

While you are not limited to using `PRIMARY KEY` fields for `block_routing_field`, the selected field must be an integer type.

The example above uses a `PRIMARY KEY` column on one table (`book`), with a one-to-many mapping to another table (`book_content`) to illustrate that what's important is that the fields selected for `block_routing_field` be same as those used in the index linking `options`.

## Potential Downsides

In terms of general searching, there really aren't any potential downsides.  Queries against block routed indexes that don't do cross-index joins will still execute just like they would if docs were routed using defaults.

Regarding indexing, however, there are a few things to keep in mind.

The primary thing is that you probably won't end up with a nearly evenly-distributed index.  Some shards will likely be measurably larger (or smaller) than the others in terms of both document counts and on-disk byte sizes.  Depending on the number of documents you're indexing, you may even see some shards be of zero size.  All of this depends on the distribution of the data in the `block_routing_field`, the number of `shards` configured, and Elasticsearch's routing hashing algorithm.  

In the future, this may necessitate that ZomboDB allow the block size of 100,000 to be configurable (or some other advanced scheme).  Howeer, right now 100,000 seems like a good number for large tables with low selectivity join queries.

Additionally, you may lose concurrency during intital `CREATE INDEX` (or `REINDEX`).  With default routing by `_id`, Elasticsearch can generally process the batch of documents being indexed in parallel across all the shards because routing by `_id` generally distributes documents evently.

With block routing, documents will be grouped together in blocks of 100,000 that all target the same shard.

When running `CREATE INDEX`, as Postgres walks the table heap and passes the documents off to the ZomboDB Index Method, if the rows on disk happen to be physically ordered by the `block_routing_field` (which likely wouldn't be uncommon when using the table's primary key for that setting), then each block of 100,000 rows will all target the same shard, essentially defeating Elasticsearch's concurrent indexing abilities.

As strange as it sounds, it would actually be better if the rows on disk were ordered randomally.  In that event you likely would't notice a decrease in indexing performance.  However, the idea of indexes is that you only need to `CREATE INDEX` once and you eat some time up-front to improve searching performance later.

Note that changing the value of `block_routing_field` via `ALTER INDEX` will necessitate a `REINDEX`.

## Should I Always Use Block Routing?

Not unless your queries almost always join to other tables and not unless those queries are low selectivity.

You'll probably find it most useful for `one-to-one` and `one-to-many`-type queries where as not so much with `many-to-one` queries.