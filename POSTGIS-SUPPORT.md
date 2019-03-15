TODO:  write this

If the `postgis` extension is already installed when you `CREATE EXTENSION zombodb` then ZDB will automatically support `postgis`.  If not, then you need to `CREATE EXTENSION postgis` and then run:

```sql
SELECT zdb.enable_postgis_support();
```

If ZomboDB was able to detect the PostGIS extension, the above will return `true`.  Otherwise it'll return `false`.

## Notes

- During indexing, ZomboDB automatically converts `geography` and `geometry` to `json` (using `ST_AsGeoJSON`) and automatically uses `ST_Translate()` to translate them to CRS `4326`
- Queries using ZDB's `dsl.geo_shape()` function need to be in CRS `4326`
- The `CONTAINS` shape relationship has been removed from Elasticsearch 6.6
- Postgres' `point` type is automatically mapped to the Elasticsearch `geo_point` type and can be queried with `dsl.geo_bounding_box()` and `dsl.geo_polygon()` queries
