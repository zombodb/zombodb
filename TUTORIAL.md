# Getting Started

Assuming you've followed the [Installation instructions](BINARY-INSTALLATION.md), using ZomboDB is extremely simple.

Since ZomboDB is an actual Postgres index type, creating and querying its indices is about as simple as any other SQL
query you might execute in Postgres.

This guide intends to demonstrate the basics using `psql`. A few assumptions I'm making about you are:

- You have a functional Postgres 10 Server
- You have a functional Elasticsearch 7.x cluster (even if just one node)
- You're familiar with Postgres and `psql`
- You're familiar with Elasticsearch on at least a high-level

ZomboDB's intent is to abstract away Elasticsearch such that it appears as any other Postgres index, so the latter
assumption isn't necessarily important.

## Create a Database and the ZomboDB Extension

Lets begin with a new database named `tutorial`.

```
$ createdb tutorial
$ psql tutorial
psql (10.1)
Type "help" for help.

tutorial=# 
```

The first thing you need to do is create the ZomboDB extension in a database.

If you're unfamiliar with Postgres extensions, spend a few minutes reading
[up on them](http://www.postgresql.org/docs/10/static/sql-createextension.html).

Now, lets create the extension:

```sql
tutorial=# 
CREATE EXTENSION zombodb;
CREATE EXTENSION
tutorial=#
```

ZomboDB installs itself into a new schema named `zdb`. It also creates a schema called `dsl` which we'll cover in the
[ZomboDB Query Lanuage](ZQL.md) and [Query Builder API](QUERY-BUILDER-API.md) documentation.

The idea here is that you would never add the `zdb` schema to your `SEARCH_PATH`, but you might want to add the `dsl`
schema for convienence while querying. This is discussed further in [QUERY-BUILDER-API.md](QUERY-BUILDER-API.md).

To prove to yourself that the extension is really installed, you can double-check the `pg_extension` system catalog:

```sql
tutorial=# 
SELECT * FROM pg_extension;
 extname | extowner | extnamespace | extrelocatable | extversion | extconfig | extcondition 
---------+----------+--------------+----------------+------------+-----------+--------------
 plpgsql |       10 |           11 | f              | 1.0        |           | 
 zombodb |       10 |         2200 | t              | 1.0.0      |           | 
(2 rows)

tutorial=# 
```

Here you can see that ZomboDB v1.0.0 is really installed.

## Create and Populate a Table

Nothing too out of the ordinary here. Lets create a simple table that might represent a product catalog.

```sql
tutorial=# 
CREATE TABLE products (
    id SERIAL8 NOT NULL PRIMARY KEY,
    name text NOT NULL,
    keywords varchar(64)[],
    short_summary text,
    long_description zdb.fulltext, 
    price bigint,
    inventory_count integer,
    discontinued boolean default false,
    availability_date date
);
CREATE TABLE
tutorial=#
```

Before we populate the table with some data, notice that the `long_description` field has a datatype of `zdb.fulltext`.

`zdb.fulltext` is a [DOMAIN type](http://www.postgresql.org/docs/10.0/static/sql-createdomain.html) that sits on top of
the standard `text` datatype. As far as Postgres is concerned, it's functionally no different than the `text` datatype,
but it has special meaning to ZomboDB when indexing and searching (which we'll discuss in a bit).

ZomboDB will automatically create an Elasticsearch mapping that will analyze fields of type `text`, including ZomboDB's
DOMAIN type `zdb.fulltext`.

Lets COPY some data into this table before we move on to creating a ZomboDB index and querying. Rather than fill this
document with boring data, just COPY it using curl:

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

In its basic form, a ZomboDB index is essentially a "covering index" that includes all the columns.

Behind the scenes, ZomboDB automatically coverts the row being indexed into JSON because JSON is the format
Elasticsearch requires.

Knowing this, lets create an index on our `products` table:

```sql
tutorial=# 
           CREATE INDEX idxproducts 
                     ON products 
                  USING zombodb ((products.*))
                   WITH (url='http://localhost:9200/');
CREATE INDEX
tutorial=# 
```

So what we've done is create an index named `idxproducts` on the `products` table, we've indicated that we want the
index to be of type `zombodb` (via `USING zombodb`) as opposed to say "btree" or "gin" or "gist", and that it should
index all columns in from the table (via, `(products.*)`).

We've also specified the URL to our Elasticsearch cluster (`WITH (url='...')`).

(a few other index options exist to control the number of Elasticsearch `shards` and `replicas` (among other things),
but we'll consider those advanced-use features and outside the scope of this document.)

When we ran `CREATE INDEX` not only did we create an index within Postgres, we also created one within Elasticsearch.

An Elasticsearch type mapping was automatically generated based on the structure of the `products` table as well.

Lets move on to querying...

## Full-text Queries

In order to ensure the ZomboDB index is used, we'll be making use of a custom operator:

- `==>` is defined as taking `::anyelement` on the left and `::zdbquery` on the right.

`::zdbquery` is a custom data type that ZomboDB installs which represents an Elasticsearch query in its QueryDSL JSON
form.

If the query isn't valid json (as shown below), then it is automatically considered to be a [ZQL](ZQL.md) query.

Building Elasticsearch QueryDSL can be complicated, but ZomboDB provides an entire set of
[SQL-based builder functions](QUERY-BUILDER-API.md) to make this process simple and type-safe.

A typical query might be:

```sql
tutorial=# 
SELECT * FROM products WHERE products ==> 'sports, box';
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
 EXPLAIN SELECT * FROM products WHERE products ==> 'sports, box';
                                  QUERY PLAN                                  
------------------------------------------------------------------------------
 Index Scan using idxproducts on products  (cost=0.00..4.06 rows=4 width=153)
   Index Cond: (products.* ==> 'sports or box'::zdbquery)
(2 rows)

tutorial=# 
```

From here, it's just a matter of coming up with a full-text query to answer your question. See the
[Query Syntax documentation](ZQL.md) or the [DSL Query Builder documenation](QUERY-BUILDER-API.md) for details on what
the full-text query syntax can do.

## Summary

In summary, the process of getting up and running is simply:

```sql
CREATE EXTENSION zombodb;
CREATE TABLE foo ...;
<load data>
CREATE INDEX ON foo USING zombodb ((foo.*) WITH (...);
SELECT FROM foo WHERE foo ==> '...';
```
