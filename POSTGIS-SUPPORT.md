TODO:  write this

Note, to enable ZomboDB's PostGIS support, you must have the `postgis` extension already installed in your database
and then run:

```sql
SELECT zdb.enable_postgis_support();
```

If ZomboDB was able to detect the PostGIS extension, the above will return `true`.  Otherwise it'll return `false`.