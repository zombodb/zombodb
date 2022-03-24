# Type Mapping

ZomboDB provides a set of default Postgres\<-->Elasticsearch type mappings for Postgres' common data types. ZomboDB also
includes a complete set of custom Postgres DOMAINs that represent all of the foreign languages that Elasticsearch
supports.

This document attempts to explain how ZomboDB maps Postgres data types to Elasticsearch data types, how they're analyzed
(if at all), and how these things can be controlled through a combination of Postgres' type system and ZomboDB-specific
functions.

## Common Data Types

These are the default mappings:
Postgres Type | Elasticsearch JSON Mapping Definition
:-: | ---
`bytea` | `{"type": "binary"}`
`boolean` | `{"type": "boolean"}`
`smallint` | `{"type": "short"}`
`integer` | `{"type": "integer"}`
`bigint` | `{"type": "long"}`
`real` | `{"type": "float"}`
`double precision` | `{"type": "double"}`
`character varying` | `{"type": "keyword", "copy_to": "zdb_all", "normalizer": "lowercase", "ignore_above": 10922}`
`text` | `{"type": "text", "copy_to": "zdb_all", "analyzer": "zdb_standard", "fielddata": true}`
`time without time zone` | `{"type": "date", "format": "HH:mm:ss.SSSSSS", "copy_to": "zdb_all"}`
`time with time zone` | `{"type": "date", "format": "HH:mm:ss.SSSSSSZZ", "copy_to": "zdb_all"}`
`date` | `{"type": "date", "copy_to": "zdb_all"}`
`timestamp without time zone` | `{"type": "date", "copy_to": "zdb_all"}`
`timestamp with time zone` | `{"type": "date", "copy_to": "zdb_all"}`
`json` | `{"type": "nested", "include_in_parent": true}`
`jsonb` | `{"type": "nested", "include_in_parent": true}`
`inet` | `{"type": "ip", "copy_to": "zdb_all"}`
`point` | `{"type": "geo_point"}`
`zdb.fulltext` | `{"type": "text", "copy_to": "zdb_all", "analyzer": "zdb_standard"}`
`zdb.fulltext_with_shingles` | `{"type": "text", "copy_to": "zdb_all", "analyzer": "fulltext_with_shingles", "search_analyzer": "fulltext_with_shingles_search"}`
`geography` _(from postgis)_ | `{"type": "geo_shape"}`
`geometry` _(from postgis)_ | `{"type": "geo_shape"}`
`geography(Point, x)` _(from postgis)_ | `{"type": "geo_point"}`
`geometry(Point, x)` _(from postgis)_ | `{"type": "geo_point"}`

Some things to note from the above:
- Columns of type `bytea` are automatically encoded as `base64` for proper storage by Elasticsearch
- Columns of type `character varying (varchar)` are **not** analyzed by Elasticsearch. They're indexed as whole values,
  but are converted to lowercase
- Columns of type `text` **are** analyzed by Elasticsearch using its `standard` analyzer, and the individual terms are
  converted to lowercase
- Columns of type `json/jsonb` are mapped to Elasticsearch's `nested` object with a dynamic template that treats
  "string" properties as if they're of type `character varying` (ie, unanalyzed exact, lowercased values), and treats
  "date" properties as if they're dates, accepting a wide range of date formats
- Columns of type `geometry` and `geography` are automatically converted to GeoJson at index time and translated to CRS
  `4326` In all cases above, arrays of Postgres types are fully supported.

## ZomboDB's Custom DOMAIN types

ZomboDB includes a few custom Postgres DOMAIN types which can be used as column types in your tables.

`zdb.fulltext` works exactly the same way as a column of type `text`, but exists so as to provide an extra hint of
metadata to client applications indicating that the column likely contains a large amount of text.

`zdb.fulltext_with_shingles` is akin to `zdb.fulltext` but uses a 2-gram single filter for high-speed right-truncated
wildcard support.

## Language-specific DOMAIN types

As noted earlier, ZomboDB provide support for all of Elasticsearch's language analyzers, exposed as Postgres DOMAINs.
This allows you to create tables with columns of type `portuguese` or `thai`, for example. The complete set of language
domains is:

```
arabic, armenian, basque, brazilian, bulgarian, catalan, chinese, cjk, 
czech, danish, dutch, english, finnish, french, galician, german, greek, 
hindi, hungarian, indonesian, irish, italian, latvian, norwegian, persian, 
portuguese, romanian, russian, sorani, spanish, swedish, turkish, thai
```

More details on how Elasticsearch analyzes each of these can be found in its
[language analyzers](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-lang-analyzer.html)
documentation.

# Defining Custom Analyzers, Filters, Normalizers, Type Mappings

ZomboDB provides a set of SQL-level functions that allow you to define custom analyzer chains, filters, normalizers,
along with custom type mappings.

These are designed to be used with Postgres' `CREATE DOMAIN` command where the domain name exactly matches the analyzer
name.

## Analysis Definition Functions

```sql
FUNCTION zdb.define_analyzer(name text, definition json)
```

Allows for the definition of Elasticsearch
[custom analyzers](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-custom-analyzer.html).
Depending on the complexity of the analyzer you need to define, you'll likely first need to define custom filters or
tokenizers, as described below.

In order to use a custom analyzer you must make a custom Postgres DOMAIN with the same name, and then you can use that
DOMAIN as column type in your tables.

You can also use the custom analyzer in conjunction with a custom field mapping via the `zdb.define_field_mapping()`
function described below.

Note that making changes to any of the analysis definitions will require a `REINDEX` of any indices that use the things
you changed.

______________________________________________________________________

```sql
FUNCTION zdb.define_filter(name text, definition json)
```

Allows for the definition of a custom Elasticsearch
[token filter](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-tokenfilters.html).

______________________________________________________________________

```sql
FUNCTION zdb.define_char_filter(name text, definition json) 
```

Allows for the definition of a custom Elasticsearch
[character filter](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-charfilters.html).

______________________________________________________________________

```sql
FUNCTION zdb.define_tokenizer(name text, definition json)
```

Allows for the definition of a custom Elasticsearch
[tokenizer](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-tokenizers.html).

______________________________________________________________________

```sql
FUNCTION zdb.define_normalizer(name text, definition json) 
```

Allows for the definition of a custom Elasticsearch
[normalizer](https://www.elastic.co/guide/en/elasticsearch/reference/current/analysis-normalizers.html).

______________________________________________________________________

```sql
FUNCTION zdb.define_type_mapping(type_name regtype, definition json)
```

If you need to define a type mapping for a Postgres datatype that isn't included in ZomboDB's defaults (`citext`, for
example), this is the function to use.

```sql
FUNCTION zdb.define_type_mapping(type_name regtype, funcid regproc)
```

If you need to define a type mapping for a Postgres datatype that isn't included in ZomboDB's defaults and if doing so
requires looking at the `typmod` value as it's defined for the column being indexed, use this function.

The first argument is the type name (as a `regtype`) and the second argument is the `regproc` oid of the function ZDB
should call to generate the mapping.

That function should take two arguments, the first being of type `regtype` and the second being an integer that is the
`typmod` value.

## Field-Specific Mapping Functions

Rather than using DOMAIN types to map Postgres types to an Elasticsearch analyzer, you can also define field-specific
mappings per table and field.

This approach can be quite powerful as you can set, per field, all the mapping properties that Elasticsearch allows, and
you don't need to create and manage custom DOMAIN types.

```sql
FUNCTION zdb.define_field_mapping(table_name regclass, field_name text, definition json) 
```

If you need to define a field mapping for a specific field in a specific table, this is the function to use. You can
specify any
[custom mapping definition json](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-params.html)
that is supported by Elasticsearch.

Creating or changing a field mapping requires a `REINDEX` of the specified table.

______________________________________________________________________

```sql
FUNCTION zdb.define_es_only_field(table_name regclass, field_name text, definition json)
```

If you want a custom field that only exists in the Elasticsearch index (perhaps as a target to the mapping
[`copy_to`](https://www.elastic.co/guide/en/elasticsearch/reference/current/copy-to.html) property, you can use this.

Any field you create here can be searched and used with aggregates, but won't be SELECT-able by Postgres.

Creating or changing an Elasticearch-only field requires a `REINDEX` of the specified table.

## Example

```sql
SELECT zdb.define_tokenizer('example_tokenizer', '{
          "type": "pattern",
          "pattern": "_"
        }');
SELECT zdb.define_analyzer('example', '{
          "tokenizer": "example_tokenizer"
        }');
CREATE DOMAIN example AS text;
CREATE TABLE foo (
   id serial8,
   some_field example
);
CREATE INDEX idxfoo ON foo USING zombodb ((foo.*));
INSERT INTO foo (some_field) VALUES ('this_is_a_test');
SELECT * FROM foo WHERE foo ==> 'some_field:this';
```

# Testing Analyzers

ZomboDB provides a few functions that can be used to evaluate how an analyzer actually tokenizes text.

```sql
FUNCTION zdb.analyze_with_field(
	index regclass, 
	field text, 
	text text) 
RETURNS TABLE (
	type text, 
	token text, 
	"position" int, 
	start_offset int, 
	end_offset int)
```

This function allows you to evaluate text analysis using the analyzer already defined for a particular field.

Examples:

```sql
SELECT * FROM zdb.analyze_with_field('idxproducts', 'keywords', 'this is a test');
 type |     token      | position | start_offset | end_offset 
------+----------------+----------+--------------+------------
 word | this is a test |        0 |            0 |         14
(1 row)
```

```sql
SELECT * FROM zdb.analyze_with_field('idxproducts', 'long_description', 'this is a test');
    type    | token | position | start_offset | end_offset 
------------+-------+----------+--------------+------------
 <ALPHANUM> | this  |        0 |            0 |          4
 <ALPHANUM> | is    |        1 |            5 |          7
 <ALPHANUM> | a     |        2 |            8 |          9
 <ALPHANUM> | test  |        3 |           10 |         14
(4 rows)
```

______________________________________________________________________

```sql
FUNCTION zdb.analyze_text(
	index regclass, 
	analyzer text, 
	text text) 
RETURNS TABLE (
	type text, 
	token text, 
	"position" int, 
	start_offset int, 
	end_offset int)
```

This function allows you to evaluate analysis using a specific analyzer name, either built-in to Elasticsearch or one of
the custom analyzers you may have defined.

Examples:

```sql
SELECT * FROM zdb.analyze_text('idxproducts', 'keyword', 'this is a test');
 type |     token      | position | start_offset | end_offset 
------+----------------+----------+--------------+------------
 word | this is a test |        0 |            0 |         14
```

```sql
SELECT * FROM zdb.analyze_text('idxproducts', 'standard', 'this is a test');
    type    | token | position | start_offset | end_offset 
------------+-------+----------+--------------+------------
 <ALPHANUM> | this  |        0 |            0 |          4
 <ALPHANUM> | is    |        1 |            5 |          7
 <ALPHANUM> | a     |        2 |            8 |          9
 <ALPHANUM> | test  |        3 |           10 |         14
```

______________________________________________________________________

```sql
FUNCTION zdb.analyze_custom(
	index regclass, 
	text text DEFAULT NULL, 
	tokenizer text DEFAULT NULL, 
	normalizer text DEFAULT NULL, 
	filter text[] DEFAULT NULL, 
	char_filter text[] DEFAULT NULL) 
RETURNS TABLE (
	type text, 
	token text, 
	"position" int, 
	start_offset int, 
	end_offset int)
```

This function allows you to dynamically define a custom analyzer and test it in real-time.

Example:

```sql
SELECT * FROM zdb.analyze_custom(
	index=>'idxproducts', 
	text=>'This is a test, 42 https://www.zombodb.com', 
	tokenizer=>'whitespace', 
	filter=>ARRAY['lowercase']);
 type |          token          | position | start_offset | end_offset 
------+-------------------------+----------+--------------+------------
 word | this                    |        0 |            0 |          4
 word | is                      |        1 |            5 |          7
 word | a                       |        2 |            8 |          9
 word | test,                   |        3 |           10 |         15
 word | 42                      |        4 |           16 |         18
 word | https://www.zombodb.com |        5 |           19 |         42
(6 rows)
```

# About Elasticsearch's `_all` Field

In short, ZomboDB disables Elasticsearch's `_all` field and instead configures its own field named `zdb_all`. By
default, all non-numeric field types are added to the `zdb_all` field.

ZomboDB does this to maintain compatability between Elasticsearch 5 and Elasticsearch 6, where
[ES 6 deprecates the `_all` field](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-all-field.html).

# Custom JSON Conversion

In general ZomboDB uses the equivalent of Postgres' `to_json()` function to convert individual columns to JSON when
indexing.\
However, ZomboDB does provide the ability to provide custom json conversion functions for any data type, and
it installs custom conversion functions for Postgres' `point` type along with PostGIS' `geometry` and `geography` types.

```sql
FUNCTION zdb.define_type_conversion(
  typeoid regtype, 
  funcoid regproc) 
RETURNS void
```

If you have a custom datatype that you need to convert to json, you need to make a conversion function that takes a
single argument that is the type you wish to convert and returns json. Then you'll call this function to associate your
type with your conversion function.

You'll also need to define a custom type mapping using `zdb.define_type_mapping()` (see above).

An example might be:

```sql
CREATE TYPE example AS (
  title varchar(255),
  description text
);

-- custom json conversion function
CREATE OR REPLACE FUNCTION example_type_to_json(example) RETURNS json IMMUTABLE STRICT LANGUAGE sql AS $$
  SELECT json_build_object('title', $1.title, 'description', $1.description);
$$;

-- associate the type with the custom json conversion function
SELECT zdb.define_type_conversion('example'::regtype, 'example_type_to_json'::regproc);

-- define a type mapping for 'example'
SELECT zdb.define_type_mapping('example'::regtype, '{"type":"nested"}');
```

Now you can create a table using that type and create a ZomboDB index on it:

```sql
CREATE TABLE test (
  id serial8 NOT NULL PRIMARY KEY,
  data example
);

CREATE INDEX idxtest ON test USING zombodb ((test.*));

INSERT INTO test (data) VALUES (('this is the title', 'this is the description'));

SELECT * FROM test WHERE test ==> dsl.nested('data', dsl.term('data.title', 'this is the title'));
 id |                      data                       
----+-------------------------------------------------
  1 | ("this is the title","this is the description")
(1 row)

```

# Similarity Module Support

ZomboDB supports
[Elasticsearch's "Similarity Module"](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules-similarity.html))
in a manner similar to the above.

This allows you to define different similarity algorithms and apply them to individual field mappings.

It uses the function

```sql
FUNCTION zdb.define_similarity(name text, definition json) 
```

An short example of defining `LMJelinekMercer` algorithm:

```sql
SELECT zdb.define_similarity('my_similarity', '
{
    "type": "LMJelinekMercer",
    "lambda": 0.075
}
');
```

And then we'd define it for a field in a table. Note that in doing so, we are required to define the entire field
mapping for this field.

```sql
CREATE TABLE test (
  id serial8 NOT NULL PRIMARY KEY,
  data text
);

SELECT zdb.define_field_mapping('test', 'data', '
{
    "type": "text",
    "analyzer": "zdb_standard",
    "similarity": "my_similarity"
}
');

CREATE INDEX idxtest ON test USING zombodb ((test.*));
```
