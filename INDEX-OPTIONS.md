# Index Options

As the [Index Management](INDEX-MANAGEMENT.md) documetation indicated, ZomboDB supports a set of custom advanced-use options for defining how an index relates to other indexes.


## Index Links

Consider that you have two tables, each with a ZomboDB index, and that your typical use case is to join the tables together in a view:

```sql
CREATE TABLE book (
   id bigserial not null primary key,
   title phrase,
   author varchar(64),
   publication_date date
);

CREATE TABLE book_content (
   book_id bigint not null primary key references book(id),
   content fulltext
);

CREATE INDEX idxbook ON book USING zombodb (zdb('book', book.ctid), zdb(book)) WITH (url='http://localhost:9200/');
CREATE INDEX idxcontent ON book_content USING zombodb (zdb('book_content', book_content.ctid), zdb(book_content)) WITH (url='http://localhost:9200/');

CREATE VIEW books_with_content AS 
   SELECT book.*, 
          book_content.content,
          zdb('book', book.ctid) AS zdb
     FROM book
LEFT JOIN book_content ON book.id = book_content.book_id;

```

Suppose you want to do a full-text query against the `books_with_content` view.  The query would be:

```sql
SELECT * FROM books_with_content WHERE zdb ==> 'author:foo and content:(beer w/3 wine w/30 cheese and food)';
```

Unfortunately, the above query will return zero rows because the index on `book` (which will be the chosen index due to the `zdb('book', book.ctid) AS zdb` column in the VIEW) doesn't have a column named `content` -- that data lives in the `book_content` table.

We need to tell the index on `book` how to find corresponding `book_content` using an "index link".  This is done through ZomboDB's index `options`:

```sql
ALTER INDEX idxbook SET (options='id=<book_content.idxcontent>book_id');
```

Now, when you run the above query, it'll be able to transparently search **both** indexes and "join" the matching data while searching.

The `options` string is a comma-separated list in the form of `local_field=<other_table.other_index>other_field`.

A maximum of 1024 comma-separated index links can be set (in the `options` property), and the relationship types (one-to-one, one-to-many, many-to-many) don't matter.

This is a powerful feature because it allows you to keep your data as normalized as you want while still providing the ability to perform full text queries across all of it.

## Naming Index Links

Index links can also be named, such that the fields behind the link appear to be part of another "object".

Taking the example from above:

```sql
ALTER INDEX idxbook SET (options='id=<book_content.idxcontent>book_id');
```

We could have, for example, named the index link `book_content`:

```sql
ALTER INDEX idxbook SET (options='book_content:(id=<book_content.idxcontent>book_id)');
```

And then the query would be:

```sql
SELECT * FROM books_with_content WHERE zdb ==> 'author:foo and book_content.content:(beer w/3 wine w/30 cheese and food)';
```

In general, this is a convenience feature for logically separating linked indexes by their domain, however it becomes more important when defining and searching `shadow` indexes.

## Further Discussion

What you're doing in the `options='...'` string is telling ZomboDB how to get from one index to another.  You're not technically describing "join conditions".  You're describing how to lookup data in a different index and relate it to the main index.

A more complex example might be:  

```
options='content:(id=<book_content.idxcontent>book_id), 
        checkout_history:(id=<checkout_history.idxcheckout_history>book_id), 
        users:(checkout_history.user_id=<users.idxusers>id)'
```

So imagine two more tables named `checkout_history` and `users` with schemas as you might expect, both of which have ZomboDB indexes.

Behind the scenes, ZomboDB builds a graph of the relationships you define, and dynamically solves how to answer your query.  It's able to see through even multiple-levels of indirection.  

With the above definition, you'd be able to find all the books checked out by a particular user:  

```sql
SELECT * 
  FROM book 
 WHERE zdb('book', book.ctid) ==> 'author:shakespeare and user.full_name:"John Doe"'
```

The relationships need not be relative to the `book` table, as shown by the `users:(checkout_history.user_id=<users.idxusers>id)` link.  ZomboDB understands that in order to get to the `users` index, it first has to go through `checkout_history`, and it knows that `checkout_history` links to `book` via the `checkout_history:(id=<checkout_history.idxcheckout_history>book_id)` link.

If you were to write the query as SQL, it would look like:

```sql
SELECT * 
 FROM book 
WHERE author ILIKE '%shakespeare%' 
  AND id IN (SELECT book_id 
               FROM checkout_history 
              WHERE user_id = (SELECT id 
                                 FROM users 
                                WHERE full_name = 'John Doe'
                               )
             );
```

Rather than setting `options='...'` on the index, ZomboDB also provides the ability to specify them at runtime.  To do that you want to make sure there's no `options='...'` property on the index, and then instead wrap the options in `#options(...)` directly in query like so:

```sql
SELECT * 
  FROM book 
 WHERE zdb('book', book.ctid) ==> '#options(content:(id=<book_content.idxcontent>book_id), 
        checkout_history:(id=<checkout_history.idxcheckout_history>book_id), 
        users:(checkout_history.user_id=<users.idxusers>id))
         author:shakespeare and user.full_name:"John Doe"'
```


# Shadow Indexes

Shadow indexes are indexes that use an existing ZomboDB index, but let you specificy different options.  This is useful if an index is used in many different SQL-level views (or JOIN) situations where different linking `options='...'` are necessary.

Shadow indexes do not consume additional disk resources, so they're "free" to create as needed.

In order to create a shadow index, you first need to make a custom UDF to take the place of the `zdb(regclass, tid)` function in the first column of the `CREATE INDEX` statement, and as the left-hand-side of the `==>` operator.  This is necessary so that Postgres will decide to use the shadow index instead of the real index.

You also need to define a new index (via `CREATE INDEX`) that specifies a `WITH` parameter named `shadow` instead of using the `url` parameter.  The value of `shadow` will be the name of an existing ZomboDB index.

## Custom `zdb(regcless, tid)` UDF

First, make a function that is defined exactly the same way as the stock `zdb(regclass, tid)` function, just with a different name:

```sql
CREATE OR REPLACE FUNCTION my_custom_zdb_func(table_name regclass, ctid tid)
 RETURNS tid
 LANGUAGE c
 IMMUTABLE STRICT
AS '$libdir/plugins/zombodb', 'zdb_table_ref_and_tid';
```

Again, this function will be used for `CREATE INDEX` and queries.

## The `shadow` Index

Next, create a shadow index:

```sql
CREATE INDEX idxshadow ON (book) 
       USING zombodb(my_custom_zdb_func('book', ctid), zdb(book)) 
        WITH (shadow='idxbook', options='<custom set of options>');
```

This index is set to use the existing index named `idxbook` and doesn't consume additional disk space or overhead when updating.  Think of it as a "view" on top of another index.

## Query 

```sql
SELECT * 
  FROM book 
 WHERE my_custom_zdb_func('book', book.ctid) ==> 'shakespeare';
```

Using the custom function you made above will allow Postgres to choose the shadow index that also uses that function (`idxshadow`) and then ZomboDB will apply the `options='...'` from the shadow index rather than the base `idxbook` index.

## Usage with Views

If you want to use this in a view, it is **required** that you include `my_custom_zdb_func(regclass, tid)` in the output list and that it be aliased `AS zdb`.  For example:

```sql
CREATE VIEW test AS 
   SELECT *, my_custom_zdb_func('book', book.ctid) AS zdb FROM book;
```

Then you can query it as:

```sql
SELECT * FROM test WHERE zdb ==> 'shakespeare';
```

And of course, the view can be as complex as you need and can include whatever other tables you might want.

It's important to remember that ZomboDB is only going to return matching rows from the base table (the table specified in the first argument to `my_custom_zdb_func()`, so you'll need to structure your view accordingly.