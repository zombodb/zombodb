# Getting Started

Assuming you've followed the [Installation](INSTALL.md) instructions, using ZomboDB is extremely simple.

Since ZomboDB is an actual Postgres index type, creating and querying full-text indexes is about as simple as any other SQL query you might execute in Postgres.

This guide intends to demonstrate the basics using ```psql```.  A few assumptions I'm making about you are:

 - You have a functional Postgres v9.5 Server
 - You have a functional Elasticsearch v1.7+ cluster (even if just one node, but not Elasticsearch 2.x)
 - You're familiar with Postgres and ```psql```
 - You're familiar with Elasticsearch on at least a high-level

ZomboDB's intent is to abstract away Elasticsearch such that it appears  as any other Postgres index, so the latter assumption isn't necessarily important.  However, we are going to discuss Elasticsearch-specific concepts like mappings and analyzers.

 
## Create a Database and the ZomboDB Extension

Lets begin with a new database named ``tutorial``.

```
$ createdb tutorial
$ psql tutorial
psql (9.5.x)
Type "help" for help.

tutorial=# 
```

The first thing you need to do is create the ZomboDB extension in the database.  If you recall from the [installation guide](INSTALL.md), the shared library must be configured to be loaded whenever a new session starts, however, it's not actually usable by a database until the extension is created.

If you're unfamiliar with Postgres extensions, spend a few minutes reading [up on them](http://www.postgresql.org/docs/9.5/static/sql-createextension.html).

Now, lets create the extension:

```sql
tutorial=# 
CREATE EXTENSION zombodb;
CREATE EXTENSION
tutorial=#
```

To prove to yourself that the extension is really installed, you can double-check the ```pg_extension``` system catalog:

```sql
tutorial=# 
SELECT * FROM pg_extension;
 extname | extowner | extnamespace | extrelocatable | extversion | extconfig | extcondition 
---------+----------+--------------+----------------+------------+-----------+--------------
 plpgsql |       10 |           11 | f              | 1.0        |           | 
 zombodb |       10 |         2200 | t              | 2.1.35     |           | 
(2 rows)

tutorial=# 
```

Here you can see that ZomboDB v2.1.35 is really installed.

## Create and Populate a Table

Nothing too out of the ordinary here.  Lets create a simple table that might represent a product catalog.

```sql
tutorial=# 
CREATE TABLE products (
    id SERIAL8 NOT NULL PRIMARY KEY,
    name text NOT NULL,
    keywords varchar(64)[],
    short_summary phrase,
    long_description fulltext, 
    price bigint,
    inventory_count integer,
    discontinued boolean default false,
    availability_date date
);
CREATE TABLE
tutorial=#
```

Before we populate the table with some data, notice that the ```short_summary``` and ```long_description``` fields have datatypes of ```phrase``` and ```fulltext``` respectively.

```phrase``` and ```fulltext``` are [DOMAIN](http://www.postgresql.org/docs/9.5/static/sql-createdomain.html)s that sit on top of the standard ```text``` datatype.  As far as Postgres is concerned, they're functionally no different than the ```text``` datatype, however they have special meaning to ZomboDB when indexing and searching (which we'll discuss in a bit).  In brief, they indicate that such fields should be analyzed.

Lets COPY some data into this table before we move on to creating a ZomboDB index and querying.  Rather than fill this document with boring data, just COPY it using curl:

```sql
tutorial=# 
COPY products FROM PROGRAM 'curl https://raw.githubusercontent.com/zombodb/zombodb/master/TUTORIAL-data.dmp';
COPY 4
tutorial=#
```

Which should give you 4 rows that look a lot like:

```
 id |      name      |                     keywords                      |                  short_summary                  |                                              long_description                                              | price | inventory_count | discontinued | availability_date 
----+----------------+---------------------------------------------------+-------------------------------------------------+------------------------------------------------------------------------------------------------------------+-------+-----------------+--------------+-------------------
  1 | Magical Widget | {magical,widget,round}                            | A widget that is quite magical                  | Magical Widgets come from the land of Magicville and are capable of things you can't imagine               |  9900 |              42 | f            | 2015-08-31
  2 | Baseball       | {baseball,sports,round}                           | It's a baseball                                 | Throw it at a person with a big wooden stick and hope they don't hit it                                    |  1249 |               2 | f            | 2015-08-21
  3 | Telephone      | {communication,primitive,"alexander graham bell"} | A device to enable long-distance communications | Use this to call your friends and family and be annoyed by telemarketers.  Long-distance charges may apply |  1899 |             200 | f            | 2015-08-11
  4 | Box            | {wooden,box,"negative space",square}              | Just an empty box made of wood                  | A wooden container that will eventually rot away.  Put stuff it in (but not a cat).                        | 17000 |               0 | t            | 2015-07-01
(4 rows)
```

## Creating an Index

In Postgres terms, a ZomboDB index is a functional index.  Before we go over how to create the index, lets discuss the functional aspects.

ZomboDB includes a function named ```zdb(record)``` (written in C) whose implementation is just a wrapper around Postgres' built-in ```row_to_json(record)``` function (although the implementation could change).

Its SQL definition looks like:
######(note that this function is installed as part of the ZomboDB extension, and there's nothing you need to do here)


```sql
tutorial=# 
\sf zdb
CREATE OR REPLACE FUNCTION public.zdb(record)
 RETURNS json
 LANGUAGE c
 IMMUTABLE STRICT
AS '$libdir/plugins/zombodb', $function$zdb_row_to_json$function$
tutorial=# 
```

Calling this function via SQL produces a JSON-formatted version of a record.  For example:

```sql
tutorial=# 
SELECT zdb(products) FROM products WHERE id = 1;
                                                                                                                                            zdb                                                                                                                                             
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 {"id":1,"name":"Magical Widget","keywords":["magical","widget"],"short_summary":"A widget that is quite magical","long_description":"Magical Widgets come from the land of Magicville and are capable of things you can't imagine","price":9900,"inventory_count":42,"discontinued":false}
(1 row)

tutorial=# 
```

ZomboDB internally uses JSON because JSON is the format Elasticsearch requires.

ZomboDB also includes a function named `zdb(regclass, tid)` which is used to statically determine table/index references in the context of sequential scans.  Both are necessary when creating an index and only the latter is used when querying.

Now that we know what `zdb(record)` and `zdb(regclass, tid)` do, lets use them to create an index:

```sql
tutorial=# 
           CREATE INDEX idx_zdb_products 
                     ON products 
                  USING zombodb(zdb('products', products.ctid), zdb(products))
                   WITH (url='http://localhost:9200/');
CREATE INDEX
tutorial=# 
```

So what we've done is create an index named ```idx_zdb_products``` on the ```products``` table, we've indicated that we want the index to be of type "zombodb" (via ```USING zombodb```) as opposed to say "btree" or "gin" or "gist", and that it should index the result of the function ```zdb(products)```.

We've also specified the URL to our Elasticsearch cluster (```WITH (url='...')```).  

(a few other index options exist to control the number of Elasticsearch ```shards``` and ```replicas``` (among other things), but we'll consider those advanced-use features and outside the scope of this document.)

When we ran CREATE INDEX not only did we create an index within Postgres, we also created one within Elasticsearch.  

The naming convention for Elasticsearch indexes is ```<db>.<schema>.<table>.<index>```, so in our case, the resulting Elasticsearch index is named ```tutorial.public.products.idx_zdb_products```.

Within the Elasticsearch index there is one type named ```data```.  It represents the actual data for the index.  

An Elasticsearch type mapping was automatically generated based on the structure of the ```products``` table, and it looks like [this](TUTORIAL-mapping.json).

Lets move on to querying...

## Full-text Queries

In order to ensure the ZomboDB index is used, we'll be making use of two things:

  - the ```zdb(regclass, tid)``` function mentioned above
  - a custom operator named ```==>```

Again, the ```zdb(regclass, tid)``` function is used to determine which index should be used.  The ```==>``` operator is ZomboDB's "full-text query" operator.

```==>``` is defined as taking `::tid` on the left and `::text` on the right:

```sql
tutorial=# 
\do ==>
                             List of operators
 Schema | Name | Left arg type | Right arg type | Result type | Description 
--------+------+---------------+----------------+-------------+-------------
 public | ==>  | json          | text           | boolean     | 
(1 row)
```

A typical query would be:

```sql
tutorial=# 
SELECT * FROM products WHERE zdb('products', products.ctid) ==> 'sports or box';
 id |   name   |           keywords            |         short_summary          |                                  long_description                                   | price | inventory_count | discontinued 
----+----------+-------------------------------+--------------------------------+-------------------------------------------------------------------------------------+-------+-----------------+--------------
  4 | Box      | {wooden,box,"negative space"} | Just an empty box made of wood | A wooden container that will eventually rot away.  Put stuff it in (but not a cat). | 17000 |               0 | t
  2 | Baseball | {baseball,sports}             | It's a baseball                | Throw it at a person with a big wooden stick and hope they don't hit it             |  1249 |               2 | f
(2 rows)

tutorial=# 
```

And its query plan is:

```sql
tutorial=# 
EXPLAIN SELECT * FROM products WHERE zdb('products', ctid) ==> 'sports or box';
                                    QUERY PLAN                                     
-----------------------------------------------------------------------------------
 Index Scan using idx_zdb_products on products  (cost=0.00..4.01 rows=1 width=153)
   Index Cond: (zdb('products'::regclass, ctid) ==> 'sports or box'::text)
(2 rows)

tutorial=# 
```

From here, it's just a matter of coming up with a full-text query to answer your question.  See the [SYNTAX](SYNTAX.md) document for details on what the full-text query syntax can do.

## Aggregations

### Terms

If, for example, you're interested in knowing the unique set of all product `keywords`, along with their occurrence count, use the `zdb_tally()` function.  

NOTE:  `zdb_tally()` only works for fields that are ***not*** of type `fulltext`.

```sql
tutorial=# 
SELECT * FROM zdb_tally('products', 'keywords', '^.*', '', 5000, 'term');
         term          | count 
-----------------------+-------
 ALEXANDER GRAHAM BELL |     1
 BASEBALL              |     1
 BOX                   |     1
 COMMUNICATION         |     1
 MAGICAL               |     1
 NEGATIVE SPACE        |     1
 PRIMITIVE             |     1
 ROUND                 |     2
 SPORTS                |     1
 SQUARE                |     1
 WIDGET                |     1
 WOODEN                |     1
(12 rows)

```

The third arugument (called the term stem) is a regular expression by which the returned terms will be filtered.  The regex above matches all the returned terms.

The fourth argument is a fulltext query by which the aggregate will be filtered.  The empty string means "no filter".  If you wanted to limit the keywords to products that are round:

```sql
tutorial=# 
SELECT * FROM zdb_tally('products', 'keywords', '^.*', 'keywords:round', 5000, 'term');
   term   | count 
----------+-------
 BASEBALL |     1
 MAGICAL  |     1
 ROUND    |     2
 SPORTS   |     1
 WIDGET   |     1
(5 rows)
```

### Significant Terms

Similar to `zdb_tally()`, the `zdb_significant_terms()` function can be used to find what Elasticsearch considers [significant terms](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-significantterms-aggregation.html).

It cannot be used with fields of type `fulltext`.

Example:

```sql
tutorial=# 
SELECT * FROM zdb_significant_terms('products', 'keywords', '^.*', '', 5000);
```

The fourth argument is a fulltext query by which the aggregate will be filtered.  The empty string means "no filter".

### Dates/Timestamps

Date/timestamp field aggregation also uses the `zdb_tally()` function, but the third argument (the term stem) can be one of `year`, `month`, `day`, `hour`, `minute`, second.  For example, to see the monthly breakdown of product availability:

```sql
tutorial=# 
SELECT * FROM zdb_tally('products', 'availability_date', 'month', '', 5000, 'term');
  term   | count 
---------+-------
 2015-07 |     1
 2015-08 |     3
(2 rows)
```

### Statistics

Use `zdb_extended_stats()` to produce statistics about a numeric field.  For example:

```sql
tutorial=# 
SELECT * FROM zdb_extended_stats('products', 'price', '');
 count | total | min  |  max  | mean | sum_of_squares |  variance  |  std_deviation   
-------+-------+------+-------+------+----------------+------------+------------------
     4 | 30048 | 1249 | 17000 | 7512 |      392176202 | 41613906.5 | 6450.88416420571
(1 row)

```

shows the overall numbers for the `price` field.  The last argument is a fulltext query that can be used to filter the set of records summarized by the aggregate:

```sql
tutorial=# 
SELECT * FROM zdb_extended_stats('products', 'price', 'round');
 count | total | min  | max  |  mean  | sum_of_squares |  variance   | std_deviation 
-------+-------+------+------+--------+----------------+-------------+---------------
     2 | 11149 | 1249 | 9900 | 5574.5 |       99570001 | 18709950.25 |        4325.5
(1 row)
```

### Term Suggestion 

The `zdb_suggest_terms()` function is used to find terms "similar" (by edit distance) to a base term in `phrase`, `phrase_array`, and `fulltext` fields.  For example:

```sql
tutorial=# 
SELECT * FROM zdb_suggest_terms('products', 'long_description', 'land', '', 5000);
 term | count 
------+-------
 LAND |     1
 LONG |     1
(2 rows)
```

The third argument (`land` in this example) is the base term.  This term will always be returned as the first value.  If the term doesn't exist in the index, its count will be zero.

The fourth argument is a fulltext query that can be used to limit the set of records that are consulted for similar terms.

The more data you have, and the more dense it is, the more effective `zdb_suggest_terms()` will be.


### Nesting aggregations via `zdb_arbitrary_aggregate()`

Aggregates can be nested following the description in [SQL-API](SQL-API.md).  An example to collect keywords by availability date is:

```sql
tutorial=# 
           SELECT * 
             FROM zdb_arbitrary_aggregate(
                         'products', 
                         $$ 
                           #tally(availability_date, month, 5000, term, 
                                           #tally(keywords, '^.*', 5000, term)
                                  ) 
                         $$, 
                         '');
 zdb_arbitrary_aggregate                                                                                                                                                                                                                                                                                                                                                                             
-------------------------
{
  "missing": {
    "doc_count": 0
  },
  "availability_date": {
    "buckets": [
      {
        "key_as_string": "2015-07",
        "key": 1435708800000,
        "doc_count": 1,
        "keywords": {
          "doc_count_error_upper_bound": 0,
          "sum_other_doc_count": 0,
          "buckets": [
            {
              "key": "box",
              "doc_count": 1
            },
            {
              "key": "negative space",
              "doc_count": 1
            },
            {
              "key": "square",
              "doc_count": 1
            },
            {
              "key": "wooden",
              "doc_count": 1
            }
          ]
        }
      },
      {
        "key_as_string": "2015-08",
        "key": 1438387200000,
        "doc_count": 3,
        "keywords": {
          "doc_count_error_upper_bound": 0,
          "sum_other_doc_count": 0,
          "buckets": [
            {
              "key": "alexander graham bell",
              "doc_count": 1
            },
            {
              "key": "baseball",
              "doc_count": 1
            },
            {
              "key": "communication",
              "doc_count": 1
            },
            {
              "key": "magical",
              "doc_count": 1
            },
            {
              "key": "primitive",
              "doc_count": 1
            },
            {
              "key": "round",
              "doc_count": 2
            },
            {
              "key": "sports",
              "doc_count": 1
            },
            {
              "key": "widget",
              "doc_count": 1
            }
          ]
        }
      }
    ]
  }
}
```

The last argument is a fulltext query that can be used to filter the set of documents against which the aggregates are run.

## Summary

In summary, the process of getting up and running is simply:

```sql
CREATE EXTENSION zombodb;
CREATE TABLE foo ...;
<load data>
CREATE INDEX ON foo USING zombodb ...;
SELECT FROM foo WHERE zdb('foo', foo.ctid) ==> ...;
```











