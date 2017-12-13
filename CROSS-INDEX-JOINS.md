# Cross-index Joins

ZomboDB's ability to do cross-index joins using what it calls "index links" is largely documented in [INDEX-OPTIONS.md](INDEX-OPTIONS.md).  This document will discuss some of the implementation details of the feature.

## "FastTerms"

"FastTerms" is a ZomboDB-specific Elasticsearch shard action that, given a query, quickly returns all terms of a specified field from matching docs (via `doc_values`).

This is akin to an Elasticsearch bucket aggregation, but with a few major differences.  First of all, no statistics are gathered (such as counting distinct values).  As such, no buckets are actually created.

Secondly, if the field from which terms are wanted is an `integer` or `long` field, FastTerms uses a [Sparse Bit Set](https://github.com/brettwooldridge/SparseBitSet) to represent the set of terms in memory and to serialize across the network.  In the case of a field of type `long` the bitset can only be used if the range of values (`max_value - min_value`) fits within 32bits.  If not, then it degrades to a sorted `long[]` for memory and network representations.

If the field is a `keyword` field, then the set of Strings is managed as a sorted `String[]`.  Analyzed fields are not supported.

Unlike Elasticsearch's various bucket aggregations, once the values are collected from each shard, they are *not* sorted together into one structure.  Instead, since the set of values from each shard can be iterated in sorted order (either by walking the contained bitset or sorted `long` or `String` arrays) the final results can be merge sorted when some part of ZomboDB needs the values sorted.

While more optimization is likely possible, the idea of FastTerms is to be low overhead so that it can quickly retrieve all the matching doc_value terms for a given field in a memory and network-transport efficient manner.

While FastTerms is the key component of ZomboDB's Cross-index join support, it is also used by ZomboDB to answer normal `SELECT ... FROM ... WHERE zdb(...)==>'...';` queries under certain circumstances.  This enables ZomboDB to stream matching tuple ids back to Postgres extremely fast -- signifcantly faster than Elasticsearch's Scan/Scroll API can do it.

## A Lucene "CrossJoinQuery" implementation

In order to actually solve queries with cross-index joins, ZomboDB also implements a Lucene Query (and associated Elasticsearch wrappers) called `CrossJoinQuery`.

To start, its JSON representation looks something like this:

```json
       "cross_join" : {                              
         "cluster_name" : "zdb-test",                
         "host" : "192.168.0.99",                            
         "port" : 9300,                              
         "index" : "tutorial.public.book_content.idxcontent",
         "type" : "data",                            
         "left_fieldname" : "id",                    
         "right_fieldname" : "book_id",                
         "can_optimize_joins" : false,                
         "query" : {                                 
           "term" : {                                
             "body" : {                          
               "value" : "beer",                     
               "boost" : 1.0                         
             }                                       
           }                                         
        }
```

Assuming we're querying a table named `books` that has an index link defined as `options='id=<book_content.idxcontent>book_id'`, and we've executed a query like `SELECT * FROM books WHERE zdb('books', ctid) ==> 'body:beer';` (where the `body` field is in the `book_content` table, the above query will be generated and executed.

What the above is doing is:

   - connecting back to the ES cluster on the transport port (`host` and `port`) -- these are automatically determined by ZomboDB
   - Uses FastTerms to
      - execute the `query` that matches `beer` in the `body` field against the index on the `book_content` table
      - collect all the `book_id` (`right_fieldname`) values from matching docs
   - executes a query against the index on `book` looking for all docs whose `id` (`left_fieldname`) field contains any of the `book_id` values found via FastTerms

When executing the final query that matches `left_fieldname` (`id`) against `right_fieldname` (`book_id`), ZomboDB will analyze the final set of values for `right_fieldname` and rewrite to a Lucene query that is as efficient as possible.  

Depending on the total amount of values and their density, ZomboDB may rewrite using a "PointInSet" query, a series of "Range" queries, a combination of both, or it may decide that's it's simply more efficient to walk all the doc_values for `left_fieldname` and look them up in the results from FastTerms, either by probing the underlying sparse bit set or a binary search if a bit set couldn't be used.

It's important to understand that when a Lucene Query is being executed by Elasticsearch, it's being executed in parallel for every primary shard on the outer index.  So if your index has 40 shards then the CrossJoinQuery is being executed 40 times, which amounts to 40 FastTerms lookups against the join index to retrieve all the results for each shard.

The `can_optimize_joins` property, which only comes in to play if the ZomboDB indexes involved are configured with compatible `block_routing_field` and `shards` settings, can ameliorate this combinatorial query execution explosion.  This is an advanced topic covered in [BLOCK-ROUTING.md](BLOCK-ROUTING.md).

## Notes on Performance

Generally speaking, cross-index joining is very fast.  In my tests with reasonable-sized data (50G indices with 10M+ rows), it out-performs (what used to be) SIREn in all of my (non-scientific) benchmarking.

It's important to remember that joining on integer keys (either 32bit or 64bit) is always going to be faster than joining on String keys -- likely by a few orders of magnitude.

Additionally, CrossJoinQuery is somewhat designed such that it'll perform faster as the number of matching join rows increases.  That may seem counter-intuitive, but assuming your join keys represent serial primary key values (which is common in relational Postgres databases), CrossJoinQuery can ultimately optimize the join such that it only needs to execute a single range query that encompasses all keys.  Obviously, this would be when the join matches *all* rows.  

And in the in-between areas where you're only joining to a subset, if that subset happens to follow the natural ordering of your primary keys, then ZomboDB is again able to reduce the query to simple range queries (range queries are extremely fast with Lucene 6.6).


