## Sequential Scan Support

ZomboDB is capable of answering fulltext queries even when Postgres decides to plan the query using a sequential scan.

An example of such a query and its plan is:

```
tutorial=# SELECT * FROM products 
           WHERE zdb('products', products.ctid) ==> 'round';
 id |      name      |        keywords         |         short_summary          |                                       long_description                                       | price | inventory_count | discontinued | availability_date 
----+----------------+-------------------------+--------------------------------+----------------------------------------------------------------------------------------------+-------+-----------------+--------------+-------------------
  1 | Magical Widget | {magical,widget,round}  | A widget that is quite magical | Magical Widgets come from the land of Magicville and are capable of things you can't imagine |  9900 |              42 | f            | 2015-08-31
  2 | Baseball       | {baseball,sports,round} | It's a baseball                | Throw it at a person with a big wooden stick and hope they don't hit it                      |  1249 |               2 | f            | 2015-08-21
(2 rows)

tutorial=# explain SELECT * FROM products 
                   WHERE zdb('products', products.ctid) ==> 'round';
                          QUERY PLAN                           
---------------------------------------------------------------
 Seq Scan on products  (cost=0.00..1.15 rows=2 width=153)
   Filter: (zdb('products'::regclass, ctid) ==> 'round'::text)
(2 rows)

```

A "Seq Scan" is a Postgres query plan construct where it evaluates the query against each row in the table, one-at-a-time.

While Postgres is determining which rows to return, ZomboDB detects the first time it sees the `zdb('products', products.ctid) ==> 'round'` query condition and queries Elasticsearch (using an endpoint exposed by ZomboDB's ES plugin).  ZomboDB then builds an in-memory hashtable (within the Postgres session backend) of all matching rows.

Once the hashtable is built, the current `ctid` of the sequential scan is looked up in the hashtable.  If it's found, that row matches.  If not, then the row doesn't match.

If you consider a sequential scan over, say, a 1M row table, there will be one query to Elasticsearch to find the matching rows, and then 1M individual hashtable lookups.