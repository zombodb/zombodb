# Type Mapping

This document attempts to explain how ZomboDB maps Postgres data types to Elasticsearch data types, how they're analyzed (if at all), and how these things can be controlled through a combination of Postgres' type system and ZomboDB-specific functions.

## Common Data Types

The mapping of common data types is straightforward.  In general, none of the core Postgres types are analyzed (or at least they behave as if they're unanalyzed).  This includes `text` and `varchar`.

### Postgres Type Conversions

Postgres Type | Elasticsearch Type
---           | ---
date          | unanalyzed-string in yyyy-MM-dd
timestamp [with time zone]    | unanalyzed-string in yyyy-MM-dd HH:mm:ss.SSSSS
time [with time zone]         | unanalyzed-string in HH:mm:ss.SSSSS
smallint      | integer
bigint        | long
float         | float
double precision | double
boolean       | boolean
text          | string, analyzed using ZomboDB's `exact` analyzer
varchar       | string, analyzed using ZomboDB's `exact` analyzer
character     | string, analyzed using ZomboDB's `exact` analyzer
uuid          | string, analyzed using ZomboDB's `exact` analyzer
json          | nested_object, where each property analyzed using ZomboDB's `exact` analyzer
unlisted type | string, analyzed using ZomboDB's `exact` analyzer

(for all types above, arrays of the Postgres type are fully supported)

### ZomboDB custom DOMAIN types

ZomboDB comes with a set of custom Postgres DOMAIN types that **are** analyzed.  `phrase`, `phrase_array`, and `fulltext` are ZomboDB-specific, while the set of 33 "language domains" map directly to Elasticsearch's language analyzers.

Domain Type | Elasticsearch Type
---         | ---
phrase      | string, analyzed using ZomboDB's `phrase` analyzer
phrase_array | array of strings, analyzed using ZomboDB's `phrase` analyzer
fulltext    | string, analyzed using ZomboDB's `phrase` analyzer
fulltext_with_shingles | string, analyzed using a 2-gram single filter for high-speed right-truncated wildcard support within "quoted phrases"
"language domains" | string, analyzed using the Elasticsearch analyzer of that name

## ZomboDB's `exact` Analyzer

ZomboDB configures an Elasticsearch analyzer named `exact` that is used to analyze all "text"-based Postgres types.  It is defined as:

```
{
   "tokenizer": "keyword",
   "filter": ["trim", "zdb_truncate_32000", "lowercase"]
}
```

(the `zdb_truncate_32000` filter truncates the value at 32,000 characters).

The intent here is to provide case-insensitive, "full-value" (ie, no tokenization) searching for "text"-based types.  If tokenization is necessary for a field, see the `phrase`/`fulltext` domains below.


## About `phrase` and `fulltext` DOMAINS

The `phrase`, `phrase_array`, and `fulltext` DOMAINS are all based on the Postgres `text` type (ie, `CREATE DOMAIN phrase AS text`).  Additionally, ZomboDB configures Elasticsearch analyzers of the same names.  All three are identically defined:

```
{
   "tokenizer": "standard",
   "filter": ["lowercase"]
}
```

Generally, this works well for latin-based languages.  Note that the analyzer does **NOT** perform stemming or stop-word removal.  This is by design.  Should you also need these abilities, you should use one of the language-specific domains.

Note that fields of type `phrase` (and `phrase_array`) **are** included in Elasticsearch's `_all` field, whereas fiels of type `fulltext` are not (but are expanded at search time to include such fields).

## About `fulltext_with_shingles` DOMAIN

The `fulltext_with_shingles` DOMAIN is similar to the `fulltext` DOMAIN described above, except its underlying definition is such that it emits tokens not only as single terms, but pairs of terms (separated by the dollar sign).

When you need high-speed right-trucated wildcard support, especially when used in "quoted phrases", this is probably the data type you want to use.

Like `fulltext`, fields of this type are **not** included in the `_all` field, but queries against such fields are instead expanded at search time.

## About langauge-specific DOMAINS

ZomboDB includes a set of 33 additional domains named after the language they're intented to represent.  Each DOMAIN uses the similarly-named Elasticsearch analyzer, and follows the analysis rules of that analyzer.

The set of supported languages is:

```
   arabic, armenian, basque, brazilian, 
   bulgarian, catalan, chinese, cjk, 
   czech, danish, dutch, english, 
   finnish, french, galician, german, 
   greek, hindi, hungarian, indonesian, 
   irish, italian, latvian, norwegian, 
   persian, portuguese, romanian, russian, 
   sorani, spanish, swedish, turkish, 
   thai
```

More details on how Elasticsearch analyzes each of these can be found in its [language analyzers](https://www.elastic.co/guide/en/elasticsearch/reference/1.7/analysis-lang-analyzer.html) documentation.

Note that fields of langauge-specific DOMAINS are **not** included in Elasticsearch's `_all` field, but queries against `_all` are expanded at search time to include such fields.

# Creating Custom DOMAINS and Analyzers

If none of the above DOMAINS and analyzers meet your needs, you can define your own.

ZomboDB includes a set of SQL-level functions to create an analyzer:

```FUNCTION zdb_define_char_filter(name text, definition json)```
```FUNCTION zdb_define_filter(name text, definition json)```
```FUNCTION zdb_define_analyzer(name text, definition json)```

In all cases, the `definition` argument is the JSON object definition of that particular construct.  For example, ZomboDB's `zdb_truncate_32000` filter would be defined as:

```
SELECT zdb_define_filter('zdb_truncate_32000', '{ "type": "truncate", "length":32000 }');
```

The complete set of char_filters, filters, and analyzers are configured for every index you create.  Once you've defined everything necessary to build your custom analyzer, you must create a domain (ie, `CREATE DOMAIN foo AS text`) on top of Postgres' `text` type (can also be on top of `varchar(<length>)` with the **same name as the analyzer**.  Then you're free to use it as a type in your tables.

The Elasticsearch [analyzer documentation](https://www.elastic.co/guide/en/elasticsearch/reference/1.7/analysis-analyzers.html) explains what the various parts of an analyzer are and how to define them in JSON.  

> The important bit here is that ZomboDB requires a Postgres `DOMAIN` with the same name.

Note that if your custom DOMAIN is defined `AS text`, fields of that type are **not** included in Elasticsearch's `_all` field, but are expanded at query time to include such fields.  However, if the custom DOMAIN is defined `AS varchar(<length>)`, the field **is** included in the `_all` field. 

## About the `_all` Field

ZomboDB enables Elasticsearch's `_all` field and it is configured to use ZomboDB's `phrase` analyzer.

The `_all` field only includes "text" and date/timestamp fields (fields of type `text`, `varchar`, `character`, `date`, `time`, `timestamp`, `json`), unless the field is of type `fulltext` or a custom DOMAIN defined `AS text`.  

If the custom DOMAIN type is defined `AS varchar(<length>)` then it **is** included in the `_all` field.

Note that for "text" fields not included in `_all`, ZomboDB expands queries to include such fields.  This means that in general, it's transparent to you that some fields aren't included in `_all` and also ensures that the proper analyzer is used at search time.  ZomboDB does **not** expand queries to non-"text" fields (ie, `integer`, `bigint`, `boolean`, etc).

# Custom Field Mappings

ZomboDB also allows you to set custom field mappings per table.field.  This begins by calling ZomboDB's `zdb_define_mapping()` function:

```
SELECT zdb_define_mapping(table_name regclass, field_name text, definition json);
```

An example for a field named `content` might be:

```
SELECT zdb_define_mapping('my_table', 'content', '{
          "store": false,
          "type": "string",
          "index_options": "positions",
          "include_in_all": "false",
          "analyzer": "fulltext",
          "fielddata": {
            "format": "disabled"
          },
          "norms": {
            "enabled": false
          }
        }');
```

Note that this is an advanced-use feature and the mapping you provide must definitely provide all the properties you might need (type, analyzer, etc) -- it completely replaces any ZomboDB-default mapping properties for the field.

The Elasticsearch [mapping documentation](https://www.elastic.co/guide/en/elasticsearch/reference/1.7/mapping.html) explains in detail what can be used here.

Note that ZomboDB **does not** store document source, and that Elasticsearch's default setting for per-field storage is false (ie, `"store":false`).  Setting `"store":true` is a waste of disk space as ZomboDB will never use it.


# How to Apply Changes to Custom Analyzers or Field Mappings

When you make a change to an existing custom analyzer/filter/char_filter or a field mapping, you need to `REINDEX INDEX index_name` any indexes that use the thing you changed.  Otherwise, your changes will not be visible to the underlying Elasticsearch index.




