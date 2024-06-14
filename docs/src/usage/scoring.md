# Scoring

ZomboDB provides a function named `zdb.score(tid) RETURNS real` that returns the score for the current matching row.  You can use it in the target list of your query and can also sort by it.

Without an `ORDER BY` clause, SQL doesn't guarantee any kind of ordering, so it's always important to also order by the score if you want the top-ranked documents first in your results.

Using the [tutorial](TUTORIAL.md) database, an example of using scores is:

```sql
tutorial=# 
   SELECT zdb.score(ctid), * 
     FROM products 
    WHERE products ==> 'sports box' 
 ORDER BY score desc;
  score   | id |   name   |               keywords               |         short_summary          |                                  long_description                                   | price | 
----------+----+----------+--------------------------------------+--------------------------------+-------------------------------------------------------------------------------------+-------+-
  1.00079 |  4 | Box      | {wooden,box,"negative space",square} | Just an empty box made of wood | A wooden container that will eventually rot away.  Put stuff it in (but not a cat). | 17000 | 
 0.698622 |  2 | Baseball | {baseball,sports,round}              | It's a baseball                | Throw it at a person with a big wooden stick and hope they don't hit it             |  1249 | 
(2 rows)
```

Note that the argument provided to `zdb.score()` is the hidden Postgres system column called `ctid`.  Internally, ZomboDB uses ctids to identify matching rows, and this is how you tell `zdb.score()` the row you want.

Also, `zdb.score()` is **not** allowed in the `WHERE` clause of queries.  It is only allowed in `ORDER BY` clauses and what Postgres calls the "target list" -- the list of columns the query should return.

If you need to limit the results of a query by score you can use ZomboDB's [`dsl.min_score()`](QUERY-DSL.md) function, or you can use a subselect of some kind, such as:

```sql
SELECT * FROM (SELECT zdb.score(ctid), * FROM products WHERE products ==> 'sports box') x WHERE x.score > 1.0;
```

But, this won't work:

```sql
# SELECT zdb.score(ctid), * FROM products WHERE products ==> 'sports box' AND zdb.score(ctid) > 1.0;
ERROR:  zdb.score() can only be used as a target entry or as a sort
```
