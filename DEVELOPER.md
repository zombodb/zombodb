There are 2 components: PostgreSQL extension and ElasticSearch plugin.

## Quick start (developing)

We are going to run and test everything.

1. Fork repo
2. git clone --depth 1 git@github.com:<user>/zombodb.git

All "create-setup-test" script is local-ci/run.sh:

```
cd local-ci
./run.sh
```


If you had some errors while running tests - look into
`postgres/results` .out files


### Example

For example you can see (maybe not anymore) an error in `postgres/results/create-indexes.out`:

```
CREATE INDEX idxso_posts ON so_posts USING zombodb (zdb('so_posts', ctid), zdb(so_posts)) WITH (url='http://localhost:9200/', bulk_concurrency=1, batch_size=1048576);
ERROR:  rc=400; {"error":{"root_cause":[{"type":"mapper_parsing_exception","reason":"Mapping definition for [_field_names] has unsupported parameters:  [index : no] [store : false]"}],"type":"mapper_parsing_exception","reason":"Failed to parse mapping [data]: Mapping definition for [_field_names] has unsupported parameters:  [index : no] [store : false]","caused_by":{"type":"mapper_parsing_exception","reason":"Mapping definition for [_field_names] has unsupported parameters:  [index : no] [store : false]"}},"status":400}
```

So we have an error while creating an index: `"reason":"Mapping
definition for [_field_names] has unsupported parameters`.

We need to look into PostgreSQL extension: postgres/src/main/c/am/elasticsearch.c

## Maven

Everything is built with Maven. So we have configuration files
(`pom.xml`) in root folder and 2 modules (`elasticsearch` and
`postgres`).

## PostgreSQL extension

TODO

## ElasticSearch plugin

After you execute "run.sh" there is .jar file and .zip file in
`elasticsearch/target` folder. The plugin is .zip.

Files inside it:

```
jackson-core-2.7.0.jar
jackson-databind-2.7.0.jar
jackson-annotations-2.7.0.jar
zombodb-plugin-3.0.0.jar
```

This resulting .zip file is described in
`elasticsearch/src/main/assemblies/plugin.xml`.

Maven configuration file (`pom.xml`) describes everything else.

For ES 2.x: there should `plugin-descriptor.properties` file inside
`src/main/resources` folder. It is included in `plugin.xml`.


If you have this error:

```
ERROR: java.lang.IllegalStateException: jar hell!
class: com.fasterxml.jackson.core.Base64Variant
jar1: /usr/share/elasticsearch/lib/jackson-core-2.6.6.jar
jar2: /tmp/5649790809676689156/temp_name-1148445711/jackson-core-2.6.6.jar
```

add `<scope>provided</scope>` to this dependecy in a `pom.xml` file.


## Tests

When you run `./run.sh` tests are run automatically.


## Notes

there is no `index_analyzer` option anymore in ES (just `analyzer`)
no store
no index
`field_names` configuration is limited to disabling the field.


This relates to ObjectMapper errors (access): https://github.com/wikimedia/search-repository-swift/issues/20



"TooManyClauses":




nosuchmethod: "com.fasterxml.jackson.core.JsonGenerator.writeStartObject(Ljava/lang/Object;)V"
It looks like the problem is that you are getting incompatible versions of jackson-core and jackson-databind - jackson-core 2.0.5 is being pulled in, but I believe at least 2.1.0 is required. 
