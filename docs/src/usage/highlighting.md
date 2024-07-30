# Highlighting

Similar to [scoring support](./scoring.md), ZomboDB can returning highlighted fragments from fields that support it (typically text fields that use an analyzer).  The function is called `zdb.highlight(tid, fieldname [, json_highlight_descriptor]) RETURNS text[]`.

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

Similarly to `zdb.score()`, the first argument to `zdb.highlight()` is the Postgres hidden system column `ctid` that identifies the row for which you want highlights.

As Elasticsearch can return multiple highlight fragments for any given field, `zdb.highlight()` returns a `text[]` which allows you to address each fragment individually.

ZomboDB uses Elasticsearch's defaults for highlighting, but if these are not sufficient for your needs, the third argument to `zdb.highlight()` allows you to set a per-field highlight definition as decribed in [Elasticsearch's highlighting documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-highlighting.html).

ZomboDB provides a type-checked helper function (also named `zdb.highlight()`) that allows you to build up a highlight definition using SQL.

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

