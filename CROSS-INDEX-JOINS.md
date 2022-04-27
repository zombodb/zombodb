# Index Options

As the [Index Management](INDEX-MANAGEMENT.md) documentation indicated, ZomboDB supports a set of custom advanced-use
options for defining how an index relates to other indexes.

## Index Links

Consider that you have two tables, each with a ZomboDB index, and that your typical use case is to join the tables
together in a view:

```sql
CREATE TABLE book (
   id bigserial not null primary key,
   title zdb.phrase,
   author varchar(64),
   publication_date date
);

CREATE TABLE book_content (
   book_id bigint not null primary key references book(id),
   content zdb.fulltext
);

CREATE INDEX idxbook ON book USING zombodb ((book.*));
CREATE INDEX idxcontent ON book_content USING zombodb ((book_content.*));

CREATE VIEW books_with_content AS 
   SELECT book.*, 
          book_content.content,
          book AS zdb
     FROM book
LEFT JOIN book_content ON book.id = book_content.book_id;

```

Suppose you want to do a full-text query against the `books_with_content` view. The query would be:

```sql
SELECT * FROM books_with_content WHERE zdb ==> 'author:foo and content:(beer w/3 wine w/30 cheese and food)';
```

Unfortunately, the above query will return zero rows because the index on `book` (which will be the chosen index due to
the `book AS zdb` column in the VIEW) doesn't have a column named `content` -- that data lives in the `book_content`
table.

We need to tell the index on `book` how to find corresponding `book_content` using an "index link". This is done through
ZomboDB's index `options`:

```sql
ALTER INDEX idxbook SET (options='id=<public.book_content.idxcontent>book_id');
```

Now, when you run the above query, it'll be able to transparently search **both** indexes and "join" the matching data
while searching.

The `options` string is a comma-separated list in the form of `local_field=<schema.other_table.other_index>other_field`.

A maximum of 1024 comma-separated index links can be set (in the `options` property), and the relationship types
(one-to-one, one-to-many, many-to-many) don't matter.

This is a powerful feature because it allows you to keep your data as normalized as you want while still providing the
ability to perform full text queries across all of it.

## Naming Index Links

Index links can also be named, such that the fields behind the link appear to be part of another "object".

Taking the example from above:

```sql
ALTER INDEX idxbook SET (options='id=<public.book_content.idxcontent>book_id');
```

We could have, for example, named the index link `book_content`:

```sql
ALTER INDEX idxbook SET (options='book_content:(id=<public.book_content.idxcontent>book_id)');
```

And then the query would be:

```sql
SELECT * FROM books_with_content WHERE zdb ==> 'author:foo and book_content.content:(beer w/3 wine w/30 cheese and food)';
```

In general, this is a convenience feature for logically separating linked indexes by their domain, however it becomes
more important when defining and searching `shadow` indexes.

## Further Discussion

What you're doing in the `options='...'` string is telling ZomboDB how to get from one index to another. You're not
technically describing "join conditions". You're describing how to lookup data in a different index and relate it to
another index.

A more complex example might be:

```
options='content:(id=<public.book_content.idxcontent>book_id), 
        checkout_history:(id=<public.checkout_history.idxcheckout_history>book_id), 
        users:(checkout_history.user_id=<public.users.idxusers>id)'
```

So imagine two more tables named `checkout_history` and `users` with schemas as you might expect, both of which have
ZomboDB indexes.

Behind the scenes, ZomboDB builds a graph of the relationships you define, and dynamically solves how to answer your
query. It's even able to see through multiple-levels of indirection.

With the above definition, you'd be able to find all the books checked out by a particular user:

```sql
SELECT * 
  FROM book 
 WHERE book ==> 'author:shakespeare and users.full_name:"John Doe"'
```

The relationships need not be relative to the `book` table, as shown by the
`users:(checkout_history.user_id=<public.users.idxusers>id)` link. ZomboDB understands that in order to get to the
`users` index, it first has to go through `checkout_history`, and it knows that `checkout_history` links to `book` via
the `checkout_history:(id=<public.checkout_history.idxcheckout_history>book_id)` link.

If you were to write the query as SQL, it would look like:

```sql
SELECT * 
 FROM book 
WHERE author ILIKE '%shakespeare%' 
  AND id IN (SELECT book_id 
               FROM checkout_history 
              WHERE user_id IN (SELECT id 
                                 FROM users 
                                WHERE full_name = 'John Doe'
                               )
             );
```

Rather than setting `options='...'` on the index, ZomboDB also provides the ability to specify them at runtime. To do
that you want to make sure there's no `options='...'` property on the index, and then instead specify the link options
using the `zdb.link_options()` function:

```sql
SELECT * 
  FROM book 
 WHERE book ==> zdb.index_options(
        ARRAY[
                'content:(id=<public.book_content.idxcontent>book_id)', 
                'checkout_history:(id=<public.checkout_history.idxcheckout_history>book_id)', 
                'users:(public.checkout_history.user_id=<users.idxusers>id)'
        ],
        'author:shakespeare and user.full_name:"John Doe"'
    );
```

# Shadow Indexes

Shadow indexes are indexes that use an existing ZomboDB index, but let you specify different `options`. This is useful
if an index is used in many different SQL-level views (or JOIN) situations where different linking `options='...'` are
desired.

Shadow indexes do not consume additional disk resources, so they're "free" to create as needed.

In order to create a shadow index, you first need to make a custom UDF to use as the first column of the `CREATE INDEX`
statement, and as the left-hand-side of the `==>` operator. This is necessary so that Postgres will decide to use the
shadow index instead of the real index.

You also need to define a new index (via `CREATE INDEX`) that specifies a `WITH` parameter named `shadow` instead of
using the `url` parameter. The `shadow` argument is simply a boolean, whose value should be `true`.

## Custom "shadow function" UDF

First, make a function that is defined exactly as below. You can change the name of the function:

```sql
CREATE OR REPLACE FUNCTION my_shadow_func(anyelement)
    RETURNS anyelement
    IMMUTABLE STRICT
    LANGUAGE c AS '$libdir/zombodb.so', 'shadow_wrapper';
```

Again, this function will be used for `CREATE INDEX` and queries.

## The `shadow` Index

Next, create a shadow index:

```sql
CREATE INDEX idxshadow ON (book) 
       USING zombodb(my_shadow_func((book.*))) 
        WITH (shadow=true, options='<custom set of options>');
```

This index is set to use the existing index named `idxbook` and doesn't consume additional disk space or overhead when
updating.\
Think of it as a "view" on top of another index.

## Query

```sql
SELECT * 
  FROM book 
 WHERE my_shadow_func(book) ==> 'shakespeare';
```

Using the custom function you made above will allow Postgres to choose the shadow index that also uses that function
(`idxshadow`) and then ZomboDB will apply the `options='...'` from the shadow index rather than the base `idxbook`
index.

## Usage with Views

If you want to use this in a view, it is **required** that you include `my_shadow_func()` in the output list and that it
be aliased `AS zdb`. For example:

```sql
CREATE VIEW test AS 
   SELECT *, my_shadow_func(book) AS zdb FROM book;
```

Then you can query it as:

```sql
SELECT * FROM test WHERE zdb ==> 'shakespeare';
```

And of course, the view can be as complex as you need and can include whatever other tables you might want.

It's important to remember that ZomboDB is only going to return matching rows from the base table (the table specified
as the argument to `my_shadow_func()`), so you'll need to structure your view accordingly.
