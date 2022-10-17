## Scoring

ZomboDB provides a function named `zdb.score(tid) RETURNS real` that returns the score for the current matching row. You
can use it in the target list of your query and can also sort by it.

Without an `ORDER BY` clause, SQL doesn't guarantee any kind of ordering, so it's always important to also order by the
score if you want the top-ranked documents first in your results.

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

Note that the argument provided to `zdb.score()` is the hidden Postgres system column called `ctid`. Internally, ZomboDB
uses ctids to identify matching rows, and this is how you tell `zdb.score()` the row you want.

Also, `zdb.score()` is **not** allowed in the `WHERE` clause of queries. It is only allowed in `ORDER BY` clauses and
what Postgres calls the "target list" -- the list of columns the query should return.

If you need to limit the results of a query by score you can use ZomboDB's [`dsl.min_score()`](QUERY-BUILDER-API.md)
function, or you can use a subselect of some kind, such as:

```sql
SELECT * FROM (SELECT zdb.score(ctid), * FROM products WHERE products ==> 'sports box') x WHERE x.score > 1.0;
```

But, this won't work:

```sql
# SELECT zdb.score(ctid), * FROM products WHERE products ==> 'sports box' AND zdb.score(ctid) > 1.0;
ERROR:  zdb.score() can only be used as a target entry or as a sort
```

## Highlighting

Similar to scoring support, ZomboDB can returning highlighted fragments from fields that support it (typically text
fields that use an analyzer). The function is called
`zdb.highlight(tid, fieldname [, json_highlight_descriptor]) RETURNS text[]`.

Using the [tutorial](TUTORIAL.md) database, an example of highlighting is:

```sql
tutorial=# 
     SELECT zdb.score(ctid), zdb.highlight(ctid, 'long_description'), long_description 
      FROM products 
     WHERE products ==> 'wooden person' 
  ORDER BY score desc;
  score   |                                            highlight                                             |                                  long_description                                  
----------+--------------------------------------------------------------------------------------------------+------------------------------------------------------------------------------------
 0.882384 | {"Throw it at a <em>person</em> with a big <em>wooden</em> stick and hope they don't hit it"}    | Throw it at a person with a big wooden stick and hope they don't hit it
 0.224636 | {"A <em>wooden</em> container that will eventually rot away.  Put stuff it in (but not a cat)."} | A wooden container that will eventually rot away.  Put stuff it in (but not a cat).
(2 rows)
```

Similarly to `zdb.score()`, the first argument to `zdb.highlight()` is the Postgres hidden system column `ctid` that
identifies the row for which you want highlights.

As Elasticsearch can return multiple highlight fragments for any given field, `zdb.highlight()` returns a `text[]` which
allows you to address each fragment individually.

ZomboDB uses Elasticsearch's defaults for highlighting, but if these are not sufficient for your needs, the third
argument to `zdb.highlight()` allows you to set a per-field highlight definition as decribed in
[Elasticsearch's highlighting documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-highlighting.html).

ZomboDB provides a type-checked helper function (also named `zdb.highlight()`) that allows you to build up a highlight
definition using SQL.

```sql
CREATE TYPE esqdsl_highlight_type AS ENUM ('unified', 'plain', 'fvh');
CREATE TYPE esqdsl_fragmenter_type AS ENUM ('simple', 'span');
CREATE TYPE esqdsl_encoder_type AS ENUM ('default', 'html');
CREATE TYPE esqdsl_boundary_scanner_type AS ENUM ('chars', 'sentence', 'word');

FUNCTION highlight(
    type zdb.esqdsl_highlight_type DEFAULT NULL,
    require_field_match boolean DEFAULT false,
    number_of_fragments int DEFAULT NULL,
    highlight_query zdbquery DEFAULT NULL,
    pre_tags text[] DEFAULT NULL,
    post_tags text[] DEFAULT NULL,
    tags_schema text DEFAULT NULL,
    no_match_size int DEFAULT NULL,

    fragmenter zdb.esqdsl_fragmenter_type DEFAULT NULL,
    fragment_size int DEFAULT NULL,
    fragment_offset int DEFAULT NULL,
    force_source boolean DEFAULT true,
    encoder zdb.esqdsl_encoder_type DEFAULT NULL,
    boundary_scanner_locale text DEFAULT NULL,
    boundary_scan_max int DEFAULT NULL,
    boundary_chars text DEFAULT NULL,
    phrase_limit int DEFAULT NULL,

    matched_fields boolean DEFAULT NULL,
    "order" text DEFAULT NULL) 
RETURNS json
```

An example usage of this function, where we change the pre/post highlight tags is:

```sql
SELECT zdb.score(ctid), 
       zdb.highlight(ctid, 
                     'long_description', 
                     zdb.highlight(pre_tags=>'{<b>}', post_tags=>'{</b>}')
                    ),
       long_description                                             
 FROM products
WHERE products ==> 'wooden person'
ORDER BY score desc;
```

Which results in:

```sql
  score   |                                           highlight                                            |                                  long_description                                   
----------+------------------------------------------------------------------------------------------------+-------------------------------------------------------------------------------------
 0.882384 | {"Throw it at a <b>person</b> with a big <b>wooden</b> stick and hope they don't hit it"}      | Throw it at a person with a big wooden stick and hope they don't hit it
 0.224636 | {"A <b>wooden</b> container that will eventually rot away.  Put stuff it in (but not a cat)."} | A wooden container that will eventually rot away.  Put stuff it in (but not a cat).
(2 rows)
```

Akin to the `zdb.highlight()` function there is also `zdb.highlight_all_fields()`, which works the same but takes no "field name" argument. 
Its definition is:

```sql
FUNCTION zdb.highlight_all_fields(
    "ctid" tid,
    "_highlight_definition" json DEFAULT zdb.highlight()
) RETURNS json
```

And using a similar example to above, returns the following:

```sql
# SELECT zdb.score(ctid),
         zdb.highlight_all_fields(ctid, zdb.highlight(pre_tags=>'{<b>}', post_tags=>'{</b>}')),
         long_description
  FROM products
  WHERE products ==> 'wooden person or box'
  ORDER BY score desc;
-[ RECORD 1 ]--------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
score                | 0.5753641724586487
highlight_all_fields | {"long_description":["Throw it at a <b>person</b> with a big <b>wooden</b> stick and hope they don't hit it"],"zdb_all":["Throw it at a <b>person</b> with a big <b>wooden</b> stick and hope they don't hit it"]}
long_description     | Throw it at a person with a big wooden stick and hope they don't hit it
-[ RECORD 2 ]--------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
score                | 0.4520718455314636
highlight_all_fields | {"long_description":["A <b>wooden</b> container that will eventually rot away.  Put stuff it in (but not a cat)."],"zdb_all":["<b>wooden</b>","<b>box</b>","<b>Box</b>","A <b>wooden</b> container that will eventually rot away.  Put stuff it in (but not a cat).","Just an empty <b>box</b> made of wood"],"keywords":["<b>wooden</b>","<b>box</b>"],"short_summary":["Just an empty <b>box</b> made of wood"],"name":["<b>Box</b>"]}
long_description     | A wooden container that will eventually rot away.  Put stuff it in (but not a cat).
```