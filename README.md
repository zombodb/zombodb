# ZomboDB [![[Build Status](https://travis-ci.org/zombodb/zombodb/branches)](https://travis-ci.org/zombodb/zombodb.svg?branch=master)](https://travis-ci.org/zombodb/zombodb/branches)

ZomboDB is a Postgres extension that enables efficient full-text searching via the use of indexes  
backed by Elasticsearch.  In order to achieve this, ZomboDB implements Postgres' [Access Method API](http://www.postgresql.org/docs/9.3/static/indexam.html).

In practical terms, a ZomboDB index is no different than a standard btree index.  As such, standard SQL commands are fully supported, including `SELECT`, `BEGIN`, `COMMIT`, `ABORT`, `INSERT`, `UPDATE`, `DELETE`, `COPY`, and `VACUUM`.

Because ZomboDB implements Postgres' Access Method API, ZomboDB indexes are MVCC-safe, even as concurrent sessions mutate underlying tables.  Following Postgres' MVCC rules means that every transaction sees, at all times, a view of the backing Elasticsearch index that is always consistent with its view of Postgres' tables.

Behind the scenes, ZomboDB indexes communicate with Elasticsearch via HTTP and are automatically synchronized, in batch, when data changes.

Index management happens using standard Postgres SQL commands such as `CREATE INDEX`, `REINDEX`, and `ALTER INDEX`.  Searching uses standard SQL `SELECT` statements with a custom operator that exposes a [full-text query language](SYNTAX.md) supporting most of Elasticsearch's query constructs.

Elasticsearch-calculated aggregations are also provided through custom functions.


## Quick Links
   - [Latest Release](https://github.com/zombodb/zombodb/releases/latest)  
   - [Installation instructions](INSTALL.md)  
   - [Getting Started Tutorial](TUTORIAL.md)  
   - [Index Management](INDEX-MANAGEMENT.md), [Index Options](INDEX-OPTIONS.md), and [Type Mapping](TYPE-MAPPING.md)
   - [Query Syntax](SYNTAX.md)  
   - [SQL-level API](SQL-API.md)  
   - [Upgrading to v2.5](UPGRADING-TO-v2.5.md)

## Features

- transaction-safe full text queries
- managed & queried via standard Postgres SQL
- works with tables of any structure
- automatically creates Elasticsearch Mappings supporting most datatypes, including arrays
   - supports full set of Elasticsearch [language analyzers](https://www.elastic.co/guide/en/elasticsearch/reference/1.7/analysis-lang-analyzer.html)
   - support for [custom analyzer chains](TYPE-MAPPING.md)
   - custom [per-field mappings](TYPE-MAPPING.md)
   - json columns as nested objects for flexible schemaless sub-documents
- works with all Postgres query plans, including [sequential scans](SEQUENTIAL-SCAN-SUPPORT.md) 
- [per-row scoring](SQL-API.md#function-zdb_scoretable_name-regclass-ctid-tid-returns-float4)
- extremely fast indexing
- [record count estimation](SQL-API.md#function-zdb_estimate_counttable_name-regclass-query-text-returns-bigint)
- custom full-text query language supporting nearly all of Elasticsearch's search features, including
  - boolean operations
  - proximity (in and out of order)
  - phrases
  - wildcards
  - fuzzy terms/phrases
  - "more like this"
  - regular expressions
  - inline scripts
  - range queries
  - term/phrase boosting
- query results expansion and [index linking](INDEX-OPTIONS.md)
- [search multiple tables at once](SQL-API.md#function-zdb_multi_searchtable_names-regclass-user_identifiers-text-query-text-returns-setof-zdb_multi_search_response)
- [high-performance hit highlighting](SQL-API.md#function-zdb_highlighttable_name-regclass-es_query-text-where_clause-text-returns-set-of-zdb_highlight_response)
- access to many of Elasticsearch's aggregations, including ability to nest aggregations
- use whatever method you currently use for talking to Postgres (JDBC, DBI, libpq, etc)
- extensive test suite

Not to suggest that these things are impossible, but there's a small set of non-features too:

- ZomboDB indexes are not WAL-logged by Postgres.  As such, are not recoverable in the event of a Postgres server crash
- interoperability with various Postgres replication schemes is unknown
- Postgres [HOT](http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/backend/access/heap/README.HOT;hb=HEAD) updates not supported
- `VACUUM FREEZE` (and wrap-around avoidance vacuums via autovacuum) will leave ZomboDB indexes in an inconsistent state

## History

The name is an homage to [zombo.com](http://zombo.com/) and its long history of continuous self-affirmation. 

Development began in 2013 by [Technology Concepts & Design, Inc](http://www.tcdi.com) as a closed-source effort to provide transaction safe full-text searching on top of Postgres tables.  While Postgres' "tsearch" features are useful, they're not necessarily adequate for 200 column-wide tables with 100M rows, each containing large text content.

Initially designed on-top of Postgres' Foreign Data Wrapper API, ZomboDB quickly evolved into an index type (Access Method) so that queries are MVCC-safe and standard SQL can be used to query and manage indexes.

Elasticsearch was chosen as the backing search index because of its horizontal scaling abilities, performance, and general ease of use.

Two years later, it's been used in production systems for quite some time and is now open-source.


## How to Use It

Usage is really quite simple.  Note that this is just a brief overview.  See the various documentation files for more detailed information.

Install the extension:

```
CREATE EXTENSION zombodb;
```

Create a table:

```
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
-- insert some data
```

Index it:

```
CREATE INDEX idx_zdb_products 
                     ON products 
                  USING zombodb(zdb('products', products.ctid), zdb(products))
                   WITH (url='http://localhost:9200/', shards=5, replicas=1);
```

Query it:

```
SELECT * FROM products WHERE zdb('products', ctid) ==> 'keywords:(sports,box) or long_description:(wooden w/5 away) and price < 100000';
```


## What you need

Product       | Version 
---           | ---      
Postgres      | 9.3
Elasticsearch | 1.7.1+ (not 2.0)
Java JDK      | 1.7.0_51+

For information about how to develop/build ZomboDB, see the [Development Guide](DEVELOPER.md).

## Credit and Thanks

Credit goes to Technology Concepts & Design, Inc, its management, and its development and quality assurance teams not only for their work during the early development days but also for their on-going support now that ZomboDB is open-source.


## Contact Information

- [Eric Ridge](mailto:eebbrr@gmail.com)
- Google Group: [zombodb@googlegroups.com](mailto:zombodb@googlegroups.com)
- Twitter:  [@zombodb](https://twitter.com/zombodb) or [@eeeebbbbrrrr](https://twitter.com/eeeebbbbrrrr)
- via github Issues and Pull Requests ;)


## License

Portions Copyright 2013-2015 Technology Concepts & Design, Inc.  
Portions Copyright 2015-2016 ZomboDB, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

