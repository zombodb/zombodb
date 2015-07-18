# ZomboDB

ZomboDB is a Postgres extension that enables efficient full-text searching via the creation of indexes (CREATE INDEX) 
backed by Elasticsearch.


## Features

- transaction-safe full-text queries
- managed with standard Postgres SQL
- works with tables of any structure 
- automatically handles most datatypes (inc. arrays)
- custom full-text query language supporting nearly all of Elasticsearch's search features (inc. spans and "more like this")
- extremely fast searching and indexing
- query results expansion and index linking
- unstructured nested objects
- use whatever method you currently use for talking to Postgres (JDBC, DBI, libpq, etc)
- high-performance hit highlighting
- access to Elasticsearch's full set of aggregations
- record count estimation
- fairly extensive test suite (NB: in progress of being converted from closed-source version)

Because ZomboDB is a Postgres index type, it "just works" with SELECT, COPY, INSERT, UPDATE, DELETE, and VACUUM statements.

Not to suggest that these things couldn't be supported in the future, but there's a small set of non-features too:

- no scoring
- indexes are not crash-safe/recoverable
- interoperability with various Postgres replication schemes is unknown
- Postgres HOT updates not supported
- only supports Postgres query plans that choose IndexScans or BitmapIndexScans (the latter is also dependent on 
sufficient work_mem to avoid Recheck conditions)


## Why and History

ZomboDB development began in 2013 by [Technology Concepts & Design, Inc](http://www.tcdi.com) as a closed-source effort 
to provide transaction safe full-text searching on top of Postgres tables.  While Postgres' "tsearch" feature are useful, 
they're not necessarily adequate for 200 column-wide tables with 100M rows, each containing large text content.

The name is an homage to [zombo.com](http://zombo.com/) and its continual self-affirmation. 

Initially designed on-top of Postgres' Foreign Data Wrapper API, ZomboDB quickly evolved into an index type (Access Method) 
so that queries are MVCC-safe, standard SQL can be used to query and manage indexes, and to provide fantastic 
INSERT/UPDATE/DELETE performance.

Elasticsearch was chosen as the backing search index because of its horizontal scaling abilities, performance, and 
general ease of use.

Two years later, it's been used in production systems for quite some time and is now open-source.


## How to Use It

Usage is really quite simple.  

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
CREATE INDEX idxbooks ON books USING zombodb (zdb(books)) WITH (url='http://localhost:9200);
```

Query it:

```
SELECT * FROM books WHERE zdb(books) ==> 'title:(catcher w/3 rye) and content:"Ossenburger Memorial Wing" or author:Salinger*';
```


## What you need

### General software
Product       | Version 
---           | ---      
Postgres      | 9.3
Elasticsearch | 1.5.2+ (not 2.0)
Java JDK      | 1.7.0_51+ 
libCurl       | 7.37.1+ 
Apache Maven  | 3.0.5 

You'll also need a Postgres compatible build environment to build ZomboDB as a Postgres extension.

NOTE:  ZomboDB has only been tested on OS X and Linux.  Windows support is unknown (but likely easy to whip into shape if necessary).

### Specific Postgres Configuration
```
local_preload_libraries = 'zombodb.so'
```

### Specific Elasticsearch Configuration
```
cluster.name: <your unique clustername>
http.max_content_length: 1024mb
threadpool.bulk.queue_size: 1024
threadpool.bulk.size: 12
script.disable_dynamic: false
index.query.bool.max_clause_count: 1000000
```

## Credit and Thanks

Credit goes to Technology Concepts & Design, Inc, its management, and its development and Quality Assurance teams not only for their work during the early development days but also for their on-going support now that ZomboDB is open-source.

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