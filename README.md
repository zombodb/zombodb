# ZomboDB ![Build Status](https://travis-ci.org/zombodb/zombodb.svg?branch=master)

ZomboDB is a Postgres extension that enables efficient full-text searching via the use of indexes  
backed by Elasticsearch.  In order to achieve this, ZomboDB implements Postgres' [Access Method API](http://www.postgresql.org/docs/9.3/static/indexam.html).

In practical terms, a ZomboDB index doesn't appear to Postgres any different than a standard btree index might.  As such, standard SQL commands for mutating are data fully supported: ```INSERT```, ```UPDATE```, ```DELETE```, ```COPY```, and ```VACUUM```.

Behind the scenes, ZomboDB indexes communicate with Elasticsearch via HTTP and are automatically synchronized in an MVCC-safe manner as data in the underlying Postgres table changes.

Following Postgres' MVCC rules means that transactions see, at all times, a view of the backing Elasticsearch index that is consistent with their view of Postgres tables.

Index management happens using standard Postgres SQL commands such as ```CREATE INDEX```, ```REINDEX```, and ```ALTER INDEX```.  Searching uses standard SQL ```SELECT``` statements with a custom operator that exposes a [full-text query language](SYNTAX.md) supporting most of Elasticsearch's query constructs.


## Quick Links
   - [Latest Release](https://github.com/zombodb/zombodb/releases/latest)  
   - [Installation instructions](INSTALL.md)  
   - [Getting Started Tutorial](TUTORIAL.md)  
   - [Query Syntax](SYNTAX.md)  
   - [SQL-level API](SQL-API.md)  

## Features

- transaction-safe full-text queries
- managed & used via standard Postgres SQL
- works with tables of any structure 
- automatically creates Elasticsearch Mappings supporting most datatypes, including arrays
- json columns as nested objects for flexible schemaless sub-documents
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
- query results expansion and index linking
- extremely fast indexing
- record count estimation
- high-performance hit highlighting
- access to Elasticsearch's full set of aggregations
- use whatever method you currently use for talking to Postgres (JDBC, DBI, libpq, etc)
- fairly extensive test suite (NB: in progress of being converted from closed-source version)

Not to suggest that these things are impossible, but there's a small set of non-features too:

- no scoring
- indexes are not WAL-logged by Postgres so are not recoverable in the event of a Postgres server crash
- interoperability with various Postgres replication schemes is unknown
- ```pg_get_indexdef()``` doesn't correctly quote index options making backup restoration annoying (would require patch to Postgres)
- Postgres [HOT](http://git.postgresql.org/gitweb/?p=postgresql.git;a=blob;f=src/backend/access/heap/README.HOT;hb=HEAD) updates not supported
- only supports Postgres query plans that choose IndexScans or BitmapIndexScans (the latter is also dependent on sufficient work_mem to avoid Recheck conditions)

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
CREATE TABLE books (
	book_id serial8 NOT NULL PRIMARY KEY,
	author varchar(128),
	publication_date date,
	title phrase,     -- 'phrase' is a DOMAIN provided by ZomboDB
	content fulltext  -- 'fulltext' is a DOMAIN provided by ZomboDB
);

-- insert some data
```

Index it:

```
CREATE INDEX idxbooks ON books 
                   USING zombodb (zdb(books)) 
                   WITH (url='http://localhost:9200', shards=5, replicas=1);
```

Query it:

```
SELECT * FROM books WHERE zdb(books) ==> 'title:(catcher w/3 rye) 
                                            and content:"Ossenburger Memorial Wing" 
                                             or author:Salinger*';
```


## What you need

Product       | Version 
---           | ---      
Postgres      | 9.3
Elasticsearch | 1.5.2+ (not 2.0)
Java JDK      | 1.7.0_51+ 
libCurl       | 7.37.1+ 
Apache Maven  | 3.0.5 

You'll also need a Postgres compatible build environment to build ZomboDB's Postgres extension along with a Java 7 compatible build environment to build the Elasticsearch plugin

NOTE:  ZomboDB has only been tested on Linux and OS X.  Windows support is unknown (but likely easy to whip into shape if necessary).


## Credit and Thanks

Credit goes to Technology Concepts & Design, Inc, its management, and its development and quality assurance teams not only for their work during the early development days but also for their on-going support now that ZomboDB is open-source.


## Contact Information

- [Eric Ridge](mailto:eebbrr@gmail.com)
- Google Group: [zombodb@googlegroups.com](mailto:zombodb@googlegroups.com)
- Twitter:  @zombodb or @eeeebbbbrrrr
- via github Issues and Pull Requests ;)


## License

Portions Copyright 2013-2015 Technology Concepts & Design, Inc.  
Portions Copyright 2015 ZomboDB, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
