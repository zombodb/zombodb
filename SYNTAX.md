# Query Syntax

The ZomboDB query syntax is designed with many conveniences for text-search operations.

An example query might look like:

```
    beer wine cheese w/3 food
```

Which would find all documents that contain the words ```beer``` __and__ ```wine``` __and__ occurrences of ```cheese``` within 3 words of ```food```, regardless of the field (or fields) that contain each word.

The ```zombodb``` query syntax provides support for searching (in no particular order):

* full boolean operators (WITH, AND, OR, NOT),
* words,
* phrases,
* fielded searching,
* fuzzy words and phrases,
* value ranges,
* wildcards (left, middle, and right truncation),
* term boosting
* proximity (of word or phrase or combinations),
* scripted searching,
* query expansion,
* "more like this", and
* more!

## Boolean Expressions and Operator Precedence

The supported set of boolean operators are the standard __NOT__, __WITH__ (for searching nested objects), __AND__, and __OR__ operators along with proximity (__W/n__ or __WO/n__).  

If no operator is declared between terms, __AND__ is assumed.  Additionally, parenthetical groupings are allowed to form complex boolean expressions.

The __PROXIMITY__ operators take the highest priority, followed by __NOT__, __WITH__, __AND__, then finally __OR__.

For example, this query finds all documents which contain both ```beer``` __AND__ ```cheese``` plus any documents that contain ```wine```:

```
    wine or beer and cheese
```

It is functionally equivalent to this query:

```
    wine or (beer and cheese)
```

Whereas, this query finds all documents which contain both ```beer``` __AND__ ```cheese``` but __NOT__ ```food```, plus any documents that contain ```wine```:

```
    wine or beer and cheese not food
```

It is functionally equivalent to this query:

```
    wine or (beer and (cheese not food))
```

For convenience, each boolean operator has a single-character abbreviation:

* WITH: __%__
* AND:  __&__
* OR:  __,__
* NOT:  __!__

So taking the example above, it could be rewritten as:

```
    wine, beer & cheese !food
```

And since the __AND__ operator is the default, it could also be written as:

```
    wine, beer cheese !food
```


## Tokenization, Escaping, Case-Sensitivity, and Term Analysis

During query parsing, tokens are formed whenever a character in this set is encountered:

```
 [ "'", "\"",  ":",  "*",  "~", "?", 
   "!",  "%",  "&",  "(",  ")", ",",
   "<",  "=",  ">",  "[",  "]", "^",
   " ",  "\r", "\n", "\t", "\f" ]
```

All other characters a valid token characters.

To use one of the above characters it must be escaped using a backslash, *or* the term must be quoted.  Any character is allowed within a quoted phrase.

`foo#bar`, for example, would parse as a single term.

Terms and phrases are sub-parsed (analyzed) using the Elasticsearch-defined search analyzer for the field being searched.  Typically, analysis only happens on fields of type `phrase`, `phrase_array`, and `fulltext`.  Additionally, fields of custom `DOMAIN` types (such as `thai`, `english`, `cjk`) are also analyzed.

Token case is preserved, but may or may not be significant depending on how the underlying analyzer is defined.  All the default-supported analyzers are case-insensitive (including searching fields of type `text` and `varchar`), but a custom analyzer might decide to preserve case.

## Term and Phrase Searching

Terms and phrases are the basic search constructs for ZomboDB and are exactly what they sound like.

A term query:  ```food```  
A phrase query: ```"Now is the time"```

Phrases can be quoted using either single or double-quotes.

A unique feature of ZomboDB is that wildcards (`?`, `*`, and `~`) are allowed in "quoted phrases".  Phrases that contain wildcards are transparently rewritten as proximity queries.

## Fields, Operators, Keywords

### Fields

If a term (or phrase) is prefixed with a field name and operator, searching will be limited to that field.  For convenience, entire parenthetical groups can be prefxied with a field name.

For example: 

```
name:"John Doe" and location:unknown 
  and crime:(shoplifting, "grand-theft auto", jaywalking)
```

Without a field name, the Elasticsearch [_all](https://www.elastic.co/guide/en/elasticsearch/reference/1.6/mapping-all-field.html) field is searched.

Most of the examples that follow elide field names for (my) convienence, but know that they can be used in almost any situation a bare term or phrase is used.

### Operators

Combined with a field name, these operators allow more sophsicated searching options.

Symbol | Description 
---    | ---      
:      | field contains term
=      | field contains term (same as : )
<      | field contains terms less than value 
<=     | field contains terms less than or equal to value
>      | field contains terms greater than value
>=     | field contains terms greater than or equal to value
!=     | field does not contain term
<>     | field does not contain term (same as != )
/to/   | range query, in form of field:START /to/ END
:~     | field contains terms matching a [regular expression](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html#regexp-syntax).  Note that regular expression searches are always **case sensitive**.
:@     | ["more like this"](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html)
:@~    | ["fuzzy like this"](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-flt-field-query.html)



### Keywords

The list of keywords is very short: `with`, `and`, `or`, `not`, and `null`.  To use one of these as a search term, simply quote it.

  

## Wildcards

There are three types of wildcards.  

Symbol | Description
---    | ---
?      | any character
*      | zero or more characters
~      | post-fix only ["fuzzy"](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html) match with default fuzz of 3

The ```?``` and ```*``` wildcards can be applied anywhere within a term.  Left, middle, and right truncation is supported.  The ```~``` wildcard is post-fix only and its fuzziness factor can be adjusted.

Examples:

```be?r```:  would match beer, bear, etc  
```b*r```:  would match beer, bar, barber, etc  
```beer~```: would match beer, been, beep, etc  
```beer~2```: would match beer, bear, beep, bean, bell, etc

Special consideration is taken for criteria in the form of: ```field:*``` or ```field:?```.  They are re-written using Elasticsearch's ["exists filter"](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-filter.html).

Note that phrases can also contain wildcarded terms, such as: `field:"phrase~ w?th wild*rd"`.  In this case, the phrase is rewritten as a proximity chain such as: `field:(phrase~ w/0 w?th w/0 wild*rd)`.  See below for details on proximity searching.

Additionally, entire phrases can "fuzzy".  For example:

`"this phrase is fuzzy"~3`

This means that the words "this", "phrase", "is", and "fuzzy" must appear within three tokens of each other, but order is not important.  So it could match `"this phrase is a neat fuzzy feature"`.

## Term/Phrase Boosting

Individual terms in a query can be "boosted" in order to increase the relevancy of matching documents.  Examples for term/phrase boosting are:

`beer^2.0` or `"this is a boosted phrase"^2.0` or `apple w/2 cider^2.0 w/3 vinegar`.

A boost can also be applied to fielded searches, such as `author:Hemmingway^5.2 or author:Fitzgerald`.  This would rank documents whose author is Hemmingway higher, by an arbitrary amount.

The boost value is a floating point number and can either be between 0 and 1 (to decrease the boosting), or greater than 1 to increase the boosting.

From the Elasticsearch documentation:

```
The boost parameter is used to increase the relative weight 
of a clause (with a boost greater than 1) or decrease the 
relative weight (with a boost between 0 and 1), but the increase 
or decrease is not linear. In other words, a boost of 2 does not 
result in double the score.

Instead, the new score is normalized after the boost is applied. 
Each type of query has its own normalization algorithm, and the 
details are beyond the scope of this book. Suffice to say that 
a higher boost value results in a higher score.
```


## Proximity Searching

Proximity searching allows to indicate that terms (or phrases) should be within a certain number of tokens of each other.

The operators are __W/n__ and __WO/n__, where "n" indicates the distance.  __W/n__ means *any order* and __WO/n__ means *in order*.

Given the phrase:  ```The quick brown fox jumped over the lazy dog's back```

A proximity search in the form of: ```jumped w/2 quick``` would match the above because 

 - there are no more than two tokens between quick and jumped; and
 - order was not required

Whereas ```jumped wo/2 quick``` would *not* match because order was required.

Proximity clauses can be chained together and are evaluated right-to-left.  For example:

```quick w/2 jumped w/4 back``` is evaluated as if it were written as ```quick w/2 (jumped w/4 back)```.

Additionally, phrase proximity is supported.  For example:

```"quick brown fox" w/3 "lazy dog's back"```

Proximity operators take the highest precedence, so when combined with other boolean operators they are evaluated first.  For example:

```quick and "brown fox" w/3 "lazy dog's" and back``` is evaluated as if it were written as: ```quick and ("brown fox" w/3 "lazy dog's") and back```

Proximity clauses can be limited to specific fields as well:  ``title:catcher w/2 title:rye``.  Note that mixed fieldnames in a proximity chain is non-sensical and will produce a parse error.


## Query Expansion

Query expansion is a way for a single query to "pull in" additional records from the same index that are related to the records that match your fulltext query.

Say you have a table that holds emails:

```sql
CREATE TABLE emails (
   id serial8 not null primary key,
   subject phrase,
   body fulltext,
   from text,
   to text[],
   cc text[],
   thread_id text,
   is_parent boolean
);
```

Now say you want to find all the emails that mention `beer` in the subject, but want to return the entire thread for each matching email.  Your query would be:

`#expand<thread_id=<this.index>thread_id>(subject:beer)`

`this.index` is a literal (ie, not to be substituted with your actual index name).

What happens is that ZomboDB first finds all the emails that match `subject:beer` and internally "joins" back to the index on `thread_id = thread_id`.

What's returned are all original matching records plus their thread members.  If an original record doesn't have a `thread_id` (ie, `thread_id` is null), the record is still returned.

The left and right field names can be whatever "join condition" makes sense for the type of expansion you're trying to perform -- the fields need not be the same.

`#expand<>()` also supports an optional `#filter()` clause, that filters the expanded results before the final set is applied.  Using the same example, lets say we only want to expand to the "parent" email in all threads that discuss beer:

`#expand<thread_id=<this.index>thread_id>(subject:beer #filter(is_parent:true))`

This says "find all emails with 'beer' in the subject, expand to their parent emails, by `thread_id`, and return the combined set of records".

Note that `#expand<>()` is truly an expansion, so the results in the above example will be every email whose subject contains beer plus the parent email in their respective thread.

If you only wanted parent emails returned you'd simply add a boolean clause outside of the expansion:

`#expand<thread_id=<this.index>thread_id>(subject:beer) and is_parent:true`

In terms of ZomboDB's query parser, `#expand<>()` is an unary operator and as such can be intermixed with boolean expressions (shown above) and infinitely nested as well.  ZomboDB solves nested expansions from the bottom-up so you're always guaranteed to get the correct results.

## Nested Object Searching using WITH

ZomboDB automatically indexes fields of type `json` as "nested objects".  The boolean operator __WITH__ allows forming queries that match on individual nested objects.

For example, if you have a field named `contributor_data` with a few values such as:

```
row #1: [ 
   { "name": "John Doe", "age": 42, "location": "TX", "tags": ["active"] },
   { "name": "Jane Doe", "age": 36, "location": "TX", "tags": ["nice"] }
]

row #2: [ 
   { "name": "Bob Dole", "age": 92, "location": "KS", "tags": ["nice", "politician"] },
   { "name": "Elizabth Dole", "age": 79, "location": "KS", "tags": ["nice"] }
]

```

To find all top-level documents whose contributors are in TX and are nice:

```
contributor_data.location:TX AND contributor_data.tags:nice
```

The above finds row #1 because row #1's contributor_data structure contains elements that have a location of TX along with tags of nice.  Essentially, it found row #1 because "John Doe" matched "TX" and "Jane Doe" matched "nice".

To limit the matching to only evaluate individual elements (rather than across the entire set of elements), use the __WITH__ operator:

```
contributor_data.location:TX WITH contributor_data.tags:nice
```

The above also finds row #1, but behind the scenes it only matched the "Jane Doe" subelement, because it's the only element with a location of "TX" and a tag of "nice".

The __WITH__ operator has the same semantics as __AND__ but requires both its left and right sides to be a nested object field reference or a parenthetical boolean expression, and all field references must be against the same nested object.

