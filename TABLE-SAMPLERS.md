# Table Samplers

Postgres provides support for custom [tablesamplers](https://www.postgresql.org/docs/current/static/sql-select.html) as part of the SELECT statement.

ZomboDB provides three different table sampler implementations that allow you to sample a table using a simple text-search query, Elasticsearch's "sampler" aggregate and "diversified_sampler" aggregations.

## Sampler Functions

```sql
TABLESAMPLE zdb.query_sampler(index_name regclass, query zdbquery)
```

This is the simplest table sampler that simply returns all the rows that match the specified query.  Its results are stable per run, regardless of the `REPEATABLE(seed)` option.

---

```sql
TABLESAMPLE zdb.sampler(index_name regclass, shard_size int, query zdbquery)
```

This table sampler uses Elasticsearch's [sampler aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-sampler-aggregation.html) to sample the specified index using a specific shard size and query.

Per the Elasticsearch documentation, a few example uses cases for this sampler might be

- Tightening the focus of analytics to high-relevance matches rather than the potentially very long tail of low-quality matches
- Reducing the running cost of aggregations that can produce useful results using only samples (e.g. `zdb.significant_terms()` aggregation)

An example usage might be:

```sql
SELECT * 
  FROM table 
  TABLESAMPLE zdb.sampler('idxtable', 200, 'tags:postgres OR tags:sql');
```

The table sampler can, of course, be combined with a WHERE clause so that you're then searching the results of the sample:

```sql
SELECT * 
  FROM table 
  TABLESAMPLE zdb.sampler('idxtable', 200, 'tags:postgres OR tags:sql')
  WHERE table ==> 'zombodb';
```

The results are stable per run, regardless of the `REPEATABLE(seed)` option.

---

```sql
TABLESAMPLE zdb.diversified_sampler(index_name regclass, shard_size int, common_field_name text, query zdbquery)
```

This table sampler uses Elasticsearch's [diversified sampler aggregation](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-diversified-sampler-aggregation.html) to sample the specified index.

Per the Elasticsearch docs, this sampler can be used in siutations where you want to

- Tighten the focus of analytics to high-relevance matches rather than the potentially very long tail of low-quality matches
- Remove bias from analytics by ensuring fair representation of content from different sources
- Reduce the running cost of aggregations that can produce useful results using only samples (e.g. `zdb.significant_terms()` aggregation)

An example usage might be:

```sql
SELECT * 
  FROM table 
  TABLESAMPLE zdb.diversified_sampler('idxtable', 200, 'author', 'tags:postgres OR tags:sql');
```

The table sampler can, of course, be combined with a WHERE clause so that you're then searching the results of the sample:

```sql
SELECT * 
  FROM table 
  TABLESAMPLE zdb.diversified_sampler('idxtable', 200, 'author', 'tags:postgres OR tags:sql')
  WHERE table ==> 'zombodb';
```

The results are stable per run, regardless of the `REPEATABLE(seed)` option.
