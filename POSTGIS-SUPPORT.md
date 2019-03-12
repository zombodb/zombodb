# PostGIS Support in ZomboDB
As of version **10-1.0.5** of ZomboDB, the `postgis` datatypes of `geometry` and `geography` are supported.

##### PostGIS Requirement
PostGIS must already be installed prior to enabling PostGIS support in ZomboDB, but can also be installed after ZomboDB as noted below.

##### Installation of PostGIS prior to ZomboDB 
If the `postgis` extension is already enabled when you install the ZomboDB plugin using `CREATE EXTENSION zombodb`, then PostGIS support will automatically be enabled in ZomboDB. 

##### Installation of PostGIS after ZomboDB 
If the PostGIS plugin is installed after ZomboDB, you will need to run ```SELECT zdb.enable_postgis_support()``` to enable support for `postgis` in ZomboDB. If ZomboDB was able to detect the PostGIS extension, the above will return `true`.  Otherwise it'll return `false`.

## Supported Coordinate Reference Systems
While PostGIS supports a plethora of coordinate systems, the current release of ElasticSearch(**6.6.1**) only supports [WGS84](https://epsg.io/4326). To bridge the CRS gap between the two products, ZomboDB creates casts from `postgis`'s `geography` and `geometry` types to `json` using `ST_AsGeoJSON()` and uses `ST_Translate()` to convert coordinates from their source CRS to [WGS84](https://epsg.io/4326) for storage in the ElasticSearch index.

## Examples and Sample Data
The [sample dataset](https://github.com/zombodb/zombodb/files/2948109/sample_data_2278.zip) is loaded in the [NAD83 / Texas South Central (ftUS)](https://epsg.io/2278) CRS. It is a PG Dump from PostgreSQL 10.6 and PostGIS 2.4.

With this dataset loaded and PostGIS installed, make sure ZomboDB is installed with PostGIS support on as indicated above and create a ZomboDB index on the table by running:
```sql
CREATE INDEX sample_data_2278_zombodb
          ON sample_data_2278
       USING zombodb ((sample_data_2278.*))
        WITH (alias=sample_data_2278);
```



## Notes
- Queries using ZDB's `dsl.geo_shape()` function need to be in CRS `4326`
- The `CONTAINS` shape relationship has been removed from Elasticsearch 6.6
