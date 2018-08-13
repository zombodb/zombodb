# Low-level API

ZomboDB exposes a simple "low-level API" that allows you to directly insert documents into, and delete documents from a ZomboDB index.  The general idea for usage of this API is to directly manipulate a ZomboDB index in a transaction-safe manner and maintain MVCC-correct results without actually storing any data in Postgres.

The API consists of just two functions:

```sql
FUNCTION zdb.llapi_direct_insert(index_name regclass, data json) RETURNS void
FUNCTION zdb.llapi_direct_delete(index_name regclass, _id text) RETURNS void
```

It's important to realize the goal here is to only store data in the backing Elasticsearch index, which then becomes your source of truth for that data -- you won't have data in Postgres from which to rebuild if something bad happens to your Elasticsearch cluster.  So proper Elasticsearch backups of these indices is extremely important.

## Example 

A practical application of this API might be with an "audit" table where you want to store and query audit information in Elasticsearch, but you want your usage of the audit table to properly interact with the surrounding Postgres transaction that's generating the audit records.

For example, lets define a basic audit table:

```sql
CREATE TABLE audit (
	id serial8 NOT NULL PRIMARY KEY,
	action_type char(2),
	username varchar(64),
	when_happened timestamp DEFAULT now(),
	description text
);
```

The goal here is to be able to "insert" into this table, have the audit record added to a backing ZomboDB index, but not actually store anything in Elasticsearch.  So lets also create a rule and ZomboDB index...

```sql
    CREATE INDEX idxaudit 
              ON audit 
           USING zombodb ((audit.*)) 
           WITH (llapi=true);
           
     CREATE RULE audit_rule 
 AS ON INSERT TO audit 
      DO INSTEAD (
           SELECT zdb.llapi_direct_insert('idxaudit', to_json(NEW))
      );
```

We've created a ZomboDB index with `llapi=true` which allows the `zdb.llapi_direct_insert()/delete()` functions to be used with that index.  We've also created a RULE that instructs Postgres to instead run the `zdb.llapi_direct_insert()` function whenever we try to INSERT into the audit table.  And we're telling the `zdb.llapi_direct_insert()` function the index we want to use (`idxaudit`) and that we want to convert the `NEW` row being inserted to json.

Now we can start adding audit records:

```sql
INSERT INTO audit (action_type, username, description) 
     VALUES ('aa', 'someuser', 'this is the first audit entry');
INSERT INTO audit (action_type, username, description) 
     VALUES ('qr', 'otheruser', 'this is the second audit entry');
INSERT INTO audit (action_type, username, description) 
     VALUES ('zy', 'anotheruser', 'this is the third audit entry');
```

Because of the ON INSERT DO INSTEAD rule we placed on the audit table it's actually empty:

```sql
SELECT * FROM audit;
 id | action_type | username | when_happened | description 
----+-------------+----------+---------------+-------------
(0 rows)
```

However, the data is actually in Elasticsearch and is queryable using ZomboDB's various aggregation functions.  For example:

```sql
SELECT zdb.count('idxaudit', dsl.match_all());                                                                                                                                count 
-------
     3
(1 row)

SELECT _id, 
       (source->>'id')::bigint AS id, 
       source->>'username' AS username, 
       source->>'action_type' AS action_type, 
       (source->>'when_happened')::timestamp AS timestamp, 
       source->>'description' AS description 
  FROM zdb.top_hits_with_id('idxaudit', ARRAY['*'], dsl.match_all(), 1000);
         _id          | id |  username   | action_type |         timestamp          |          description           
----------------------+----+-------------+-------------+----------------------------+--------------------------------
 AWRH64Er5WofK_aA4sA7 |  1 | someuser    | aa          | 2018-06-28 13:42:53.734403 | this is the first audit entry
 AWRH8OG45WofK_aA4sA- |  2 | otheruser   | qr          | 2018-06-28 13:48:46.134336 | this is the second audit entry
 AWRH8OHP5WofK_aA4sA_ |  3 | anotheruser | zy          | 2018-06-28 13:48:46.157529 | this is the third audit entry
(3 rows)
```

Where this becomes most useful is when combined with a transction that does other things and then ends up aborting:

```sql
BEGIN;
INSERT INTO foo VALUES ('a', 'b', 'c');
INSERT INTO audit (action_type, username, description) VALUES ('ii', 'user', 'did a foo insert');
INSERT INTO bar VALUES ('1', '2', '3');
-- transaction aborts for some reason
ABORT;
```

Since the transaction aborted, the row inserted into `foo` won't be visible, and as such we won't have the corresponding `audit` record in the backing Elasticsearch index either.


## Interactions with autovacuum

Using our audit table example above, Postgres will never autovacuum it because the underlying Postgres heap will always be empty.  However, it *will* need to be vacuumed to ensure aborted and deleted (via `zdb.llapi_direct_delete()`) rows get removed.  So you'll need to issue direct `VACUUM table` statements regularly.
