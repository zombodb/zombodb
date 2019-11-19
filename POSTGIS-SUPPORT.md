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
There are two sample sets. 

[Sample dataset 1](https://github.com/zombodb/zombodb/files/2948109/sample_data_2278.zip) is loaded in the [NAD83 / Texas South Central (ftUS)](https://epsg.io/2278) CRS. 

[Sample dataset 2](https://github.com/zombodb/zombodb/files/3027737/sample_data_4326.zip) is loaded in the [WGS84 - World Geodetic System 1984](https://epsg.io/4326) CRS. 

Both are a PG Dump from PostgreSQL 10.6 and PostGIS 2.4.

With this dataset loaded and PostGIS installed, make sure ZomboDB is installed with PostGIS support on as indicated above and create a ZomboDB index on the table by running:
```sql
CREATE INDEX sample_data_2278_zombodb
          ON sample_data_2278
       USING zombodb ((sample_data_2278.*))
        WITH (alias=sample_data_2278);
```

and

```sql
CREATE INDEX sample_data_4326_zombodb
          ON sample_data_4326
       USING zombodb ((sample_data_4326.*))
        WITH (alias=sample_data_4326);
```

## Querying the Sample Data
The most common ways of searching across spatialized data would be searching through points using polygons and bounding boxes whether they be drawn by the user or calculated from the extent of a map on the screen. To do this we will use the Geo Polygon and Bounding Box queries as shown below.

#### Geo Polygon Query
The function used for this type of query is `dsl.geo_polygon`. It accepts arguments of `field` as a text value such as `point_to_query` and a VARIADIC of type `point`. A `point` is a string containing a comma separated `'lon, lat'` values. The query below would return all records whose geo_point field of PostGIS type `POINT` fell within the bounds of the polygon coordinates enumerated after it. As this is variadic and a polygon, it must contain at least three points and its ending latitude and longitude must be the same as its starting latitude and longitude.

```sql
SELECT * 
FROM sample_data_4326
WHERE sample_data_4326 ==> 
      dsl.geo_polygon('geo_point', 
      '-95.3757924220804,29.7530206054157', 
      '-95.3761162225586,29.753216394294', 
      '-95.3763406015772,29.7529338505327', 
      '-95.3766643966309,29.7531296379236', 
      '-95.3762156463589,29.7536947317361', 
      '-95.3758918431387,29.7534989430962', 
      '-95.3755680421945,29.7533031536912', 
      '-95.3757026673686,29.7531336250561', 
      '-95.3757924220804,29.7530206054157');
```

#### Bounding Box Query
The function used for this type of query is `dsl.geo_bounding_box`. It accepts arguments of `field` as a text value such as `point_to_query` and a string `box`. The `box` string is comprised of 4 comma separated values representing `'min lon, min lat, max lon, max lat'`. The query below would return all records whose geo_point field of PostGIS type `POINT` fell within the bounds of the box defined by the four corrdinates.

```sql
SELECT *
FROM sample_data_4326
WHERE sample_data_4326 ==>
      dsl.geo_bounding_box('geo_point',
        '-95.3757924220804,29.7530206054157,-94.3757924220804,30.7530206054157');
```

#### GeoShape Queries
Searching for points as noted above is a fairly straight-forward endeavor as you are merely searching for points inside a shape. To search for shapes such as polygons, linestrings in relation to shapes given by queries, ElasticSearch uses its GeoShape query. GeoShape queries support 4 spatial relation operators:

* INTERSECTS - (default) Return all documents whose geo_shape field intersects the query geometry.
* DISJOINT - Return all documents whose geo_shape field has nothing in common with the query geometry.
* WITHIN - Return all documents whose geo_shape field is within the query geometry.
* CONTAINS - Return all documents whose geo_shape field contains the query geometry.

In addition to the spatial relation operator, you will also supply a shape.

The two queries below show an envelope which is essentially a bounding box. However, our query will search for the column geom which is a POLYGON inside of our indexed table.

The first query will find all geom polygons that intersect with the envelope.
```sql
SELECT *
FROM sample_data_4326
WHERE sample_data_4326 ==>
      dsl.geo_shape('geom', '{"type":"envelope","coordinates":[[-95.3757924220804,29.7530206054157],[-95.3761162225586,29.753216394294]]}','INTERSECTS');
```

The second query will find all geom polygons that have no relation to the envelope int hat they are not intersecting, contained or within the envelope defined.
```sql
SELECT *
FROM sample_data_4326
WHERE sample_data_4326 ==>
      dsl.geo_shape('geom', '{"type":"envelope","coordinates":[[-95.3757924220804,29.7530206054157],[-95.3761162225586,29.753216394294]]}','DISJOINT');
```

#### GeoShape with ST_AsGeoJSON()
You can combine ZomboDB query params with PostGIS functions. For example, from the `sample_data_4326` I can take the following GeoJSON value:

```json
{"type":"MultiPolygon","coordinates":[[[[-95.3757924220804,29.7530206054157],[-95.3761162225586,29.753216394294],[-95.3763406015772,29.7529338505327],[-95.3766643966309,29.7531296379236],[-95.3762156463589,29.7536947317361],[-95.3758918431387,29.7534989430962],[-95.3755680421945,29.7533031536912],[-95.3757026673686,29.7531336250561],[-95.3757924220804,29.7530206054157]]]]}
```

With this value, I can create a query like the two in the **GeoShape Queries** section looking for geom points that intersect with this shape. However, this value was derived from the following query:

```sql
SELECT st_asgeojson((SELECT geom FROM postgis.sample_data_4326 WHERE "HCAD_NUM" = '1292500000054'))::json;
```

I can run the same query using ST_AsGeoJSON() and shorten the query considerably like so:

```sql
SELECT postgis.hcad_real_acct.*
FROM sample_data_4326
LEFT JOIN hcad_real_acct ON sample_data_4326."HCAD_NUM" = realescout.hcad_real_acct.account
WHERE sample_data_4326 ==>
      dsl.geo_shape('geom', st_asgeojson((SELECT geom FROM realescout.sample_data_4326 WHERE "HCAD_NUM" = '1292500000054'))::json,'INTERSECTS');
```

Above, we select the `geom` field encompassing it in the ST_AsGeoJSON() function and cast it as JSON to pass to the `dsl.geo_shape` query. This is nice for when you have predefined shapes in the database. For example, if I had an additional table called `zip_codes` with the geometry for all of the zip codes in the dataset stored there, I could do aggregation or selections of items in that zip code based on the shape.

## Notes
- All queries using ZDB's spatialized index data need to be in CRS `WGS84 - EPSG:4326`
- During indexing, ZomboDB automatically converts `geography` and `geometry` to `json` (using `ST_AsGeoJSON`) and automatically uses `ST_Translate()` to translate them to CRS `4326`
- Queries using ZDB's `dsl.geo_shape()` function need to be in CRS `4326`
- The `CONTAINS` shape relationship has been removed from Elasticsearch 6.6
- Postgres' `point` type is automatically mapped to the Elasticsearch `geo_point` type and can be queried with `dsl.geo_bounding_box()` and `dsl.geo_polygon()` queries
- Columns defined as `geometry(Point, x)` or `geography(Point, x)` are automatically mapped to the Elasticsearch `geo_point` type and can be queried with `dsl.geo_bounding_box()` and `dsl.geo_polygon()` queries