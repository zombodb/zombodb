## CREATE INDEX

This document explains the various methods you can use to create a ZomboDB index.

For all these examples below, lets assume we have a table defined as:

```sql
CREATE TABLE products (
    id SERIAL8 NOT NULL PRIMARY KEY,
    name text NOT NULL,
    keywords varchar(64)[],
    short_summary text,
    long_description zdb.fulltext, 
    price bigint,
    inventory_count integer,
    discontinued boolean default false,
    availability_date date,
    manufacturer_id bigint
);
```

You'll also want some familiarity with ZomboDB's [TYPE-MAPPING.md](TYPE-MAPPING.md) support along with its various [`WITH` options](INDEX-MANAGEMENT.md#with--options).

## Indexing All Columns

The simplest way to create a ZomboDB index is to simply index all the columns in the table.  Indexing all columns allows you to query any of them in Elasticsearch queries and also use any of them in ZomboDB's aggregate functions.  

```sql
CREATE INDEX idxproducts 
          ON products 
       USING zombodb ((products.*))
        WITH (...);
```

## Indexing Specific Columns

If you only wish to index, and as such only be able to query/aggregate, specific columns, you can use the Postgres `ROW()` constructor in the index definition.

First however, you must define a custom Postgres composite data type in which to cast the columns you wish to index.  This is necessary to a) define the field names you'll use for searching and also change data types if you wish.

```sql
CREATE TYPE products_idx_type AS (
	id bigint, 
	name varchar, 
	short_summary text
);	
CREATE INDEX idxproducts 
          ON products 
       USING zombodb ((
			           ROW(
			               id, 
			               name, 
			               short_summary
			              )::products_idx_type
                     ))
        WITH (...)
```

With this form, you'll only be able to search, using ZomboDB, the `id`, `name`, and `short_summary` columns.  You'll also notice that the `name` column's type has been changed from `text` to `varchar`, which (if you read the [TYPE-MAPPING.md](TYPE-MAPPING.md) documentation, will cause it be indexed as a `keyword` in Elasticesarch.

You can also perform some bit of SQL-based logic here too.  For example, if we want to include the `long_description` field too, but combined into a generic text column, we might do something like:

```sql
CREATE TYPE products_idx_type AS (
	id bigint, 
	name varchar, 
	description text
);	

CREATE INDEX idxproducts 
          ON products 
       USING zombodb ((
			           ROW(
			               id, 
			               name, 
                         COALESCE(short_summary, '') || ' ' || COALESCE(long_description, '')
			              )::products_idx_type
                     ))
        WITH (...)
```

As you can see, we concatenate the `short_summary` and `long_description` columns (guarding against NULL values `COALESCE` and separating with a space) into the field named `description`.

## Indexing with a Function

If you want to build more complex indices than the above options allow, you can index the result of a function.

The function must be a one-argument function that takes the table as its only argument and returns whatever your custom type is.

For this example, lets assume we also have a table called `manufacturer`:

```sql
CREATE TABLE manufacturer (
    id SERIAL8 NOT NULL PRIMARY KEY,
    name text NOT NULL,
    address1 text,
    address2 text,
    city varchar,
    state varchar(2),
    zip_code varchar,
    phone varchar,
    fax varchar,
    support_email varchar
);
```

And lets say when we create our ZomboDB index on `products` we want to include the product's manufacturer data with each row.  

```sql
CREATE TYPE products_idx_type AS (
	id bigint, 
	name varchar, 
	short_summary text,
	manufacturer jsonb
);
```

Now we create a function that will convert the related manufacturer information for each product into a jsonb blob:

```sql
CREATE FUNCTION products_with_manufacturer(products) RETURNS products_idx_type IMMUTABLE STRICT LANGUAGE SQL AS $$
	SELECT ROW($1.id, $1.name, $1.short_summary, (SELECT row_to_json(m) FROM manufacturer m WHERE m.id = $1.manufacturer_id))::products_idx_type;
$$;
```

And finally, we can create the ZomboDB index:

```sql
CREATE INDEX idxproducts 
          ON products 
       USING zombodb (products_with_manufacturer(products))
        WITH (...);
```	

With this, the backing Elasticsearch index will have a `nested_object` field named `manufacturer` that can be queried:

```sql
SELECT * FROM products WHERE products ==> 'manufacturer.name:Sears';
```

It's important to understand that with this example, INSERTs/UPDATEs/DELETEs to the `manufacturer` table **WILL NOT** be reflected in the ZomboDB index on `products` until the corresponding row(s) in `products` are later modified in some way.

You can manually solve this situation by applying `ON INSERT/UPDATE/DELETE` triggers on `manufacturer` that somehow "touch" all the rows in `products` that match on `manufacturer.id = products.manufacturer_id`.

### Notes

 - the function you create can be implemented in any supported Postgres `LANGUAGE`, not just `SQL` -- it could be implemented in `PLPGSQL`, `PLPERLU`, etc.
 - changes to the function's implementation (via `CREATE OR REPLACE FUNCTION`) will require that the index be reindexed using Postgres' `REINDEX INDEX` statement.

## Conclusion

These are just some simple examples.  It's up to you to decide what you want to index/query, and how.
