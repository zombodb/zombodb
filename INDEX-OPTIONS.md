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

Unfortunately, the above query will return zero rows because the index on `book` (which will be the chosen index due to the `zdb(book) AS zdb` column in the VIEW) doesn't have a column named `content` -- that data lives in the `book_content` table.

We need to tell the index on `book` how to find corresponding `book_content` using an "index link".  This is done through ZomboDB's index `options`:

```sql
ALTER INDEX idxbooks SET (options='id=<book_content.idxcontent>book_id');
```

Now, when you run the above query, it'll be able to transparently search **both** indexes and "join" the matching data while searching.

The `options` string is a comma-separated list in the form of `local_field=<other_table.other_index>other_field`.

A maximum of 1024 comma-separated index links can be set (in the `options` property), and the relationship types (one-to-one, one-to-many, many-to-many) don't matter.

This is a powerful feature because it allows you to keep your data as normalized as you want while still providing the ability to perform full text queries across all of it.

## Naming Index Links

Index links can also be named, such that the fields behind the link appear to be part of another "object".

Taking the example from above:

```sql
ALTER INDEX idxbooks SET (options='id=<book_content.idxcontent>book_id');
```

We could have, for example, named the index link `book_content`:

```sql
ALTER INDEX idxbooks SET (options='book_content:(id=<book_content.idxcontent>book_id)');
```

And then the query would be:

```sql
SELECT * FROM books_with_content WHERE zdb ==> 'author:foo and book_content.content:(beer w/3 wine w/30 cheese and food)';
```

In general, this is a convenience feature for logically separating linked indexes by their domain, however it becomes more important when defining and searching `shadow` indexes.

## Shadow Indexes

### TODO