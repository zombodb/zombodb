## Upgrading to v2.5.x

v2.5 is a major upgrade because it provides support for query plans that use sequential scans, filters, and re-check conditions.  Support for such query plans enables ZomboDB to be used in complex queries involving joins between tables and mixed "zdb" and standard SQL filter conditions.

If you have an existing ZomboDB v2.1.x installation, you will need to drop all your `USING zombodb` indexes and re-create them using the new syntax required by v2.5.x.

In v2.1.x, you would create an index like this:

```sql
CREATE INDEX idxfoo 
          ON table 
       USING zombodb (zdb(table)) 
        WITH (...);
```

In v2.5.x, you create an index like this:

```sql
CREATE INDEX idxfoo 
          ON table 
       USING zombodb(zdb('table', ctid), zdb(table))
        WITH (...);
```

The reason for this change is that in order to support query plans that choose sequential scans/filters/re-checks, ZomboDB needs to be able to statically determine which table/index to use.

As such, the SELECT syntax has a corresponding change.  In v2.1.x, the syntax was:

```sql
SELECT * FROM table 
        WHERE zdb(table) ==> 'fulltext query';
```

Whereas with v2.5.x, the syntax is:

```sql
SELECT * FROM table 
        WHERE zdb('table', table.ctid) ==> 'fulltext query';
```

It's slightly more verbose, but the added flexibility of being able to operate within sequential scans is worth it.