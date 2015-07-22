# Query Syntax

The ZomboDB query syntax is designed with many conveniences for text-search operations.

An example query might look like:

```
    beer wine cheese w/3 food
```

Which would find all documents that contain the words ```beer``` __and__ ```wine``` __and__ occurrences of ```cheese``` within 3 words of ```food```, regardless of the field (or fields) that contain each word.

The ```zombodb``` query syntax provides support for searching (in no particular order):

* full boolean operators (AND, OR, NOT),
* words,
* phrases,
* fielded searching,
* proximity (of word or phrase or combinations),
* fuzzy words and phrases,
* value ranges,
* wildcards (left, middle, and right truncation),
* scripted searching,
* query expansion,
* "more like this", and
* more!

## Boolean expressions and operator precedence

The supported set of boolean operators are the standard __NOT__, __AND__, and __OR__ operators along with proximity (__W/n__ or __WO/n__).  

If no operator is declared between terms, __AND__ is assumed.  Additionally, parenthetical groupings are allowed to form complex boolean expressions.

The __PROXIMITY__ operators take the highest priority, followed by __NOT__, __AND__, then finally __OR__.

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


## Tokenization, Escaping and Case-Sensitivity

Tokens are formed whenever a character in this set is found:

```
[" ", "\t", "\n", "\r", "\f", "$",
"*", "?", "~", "^", "/", ":", "=",  
"<", ">", "!", "#", "@", "(", ")",  
"'", "\"", ".", ",", "&", "[", "]", 
"\\"]
```

To use one of the above characters in a term, it must be escaped using a backslash, *or* the term must be quoted.  Any character is allowed within a quoted phrase.

All searching is case-insensitive.  There is no difference between ```BEER``` and ```beer```.


## Term and Phrase Searching

Terms and phrases are the basic search constructs for ZomboDB and are exactly what they sound like.

A term query:  ```food```  
A phrase query: ```"Now is the time"```

Phrases can be quoted using either single or double-quotes.


## Fields, Operators, Keywords

### Fields

If a term (or phrase) is prefixed with a field name and operator, searching will be limited to that field.  For convenience, entire parenthetical groups can be prefxied with a field name.

For example: 

```
name:"John Doe" and location:unknown 
  and crime:(shoplifting, "grand-theft auto", jaywalking)
```

Without a field name, the Elasticsearch [___all__](https://www.elastic.co/guide/en/elasticsearch/reference/1.6/mapping-all-field.html) field is searched.

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
:~     | field contains terms matcing [regular expression](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html#regexp-syntax)
:@     | ["more like this"](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-mlt-query.html)
:@~    | ["fuzzy like this"](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-flt-field-query.html)

### Keywords

The list of keywords is very short: ```and```, ```or```, ```not```, and ```null```.  To use one of these as a search term, simply quote it.

  

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
```beer~2```: would batch beer, bear, beep, bean, bell, etc

Special consideration is taken for criteria in the form of: ```field:*``` or ```field:?```.  They are re-written using Elasticsearch's ["exists filter"](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-exists-filter.html).



## Proximity Searching

Proximity searching allows to indicate that terms (or phrases) should be within a certain number of tokens of each other.

The operators are __W/n__ and __WO/n__, where "n" indicates the distance.  __W/n__ means *any order* and __WO/n__ means *in order*.

Given the phrase:  ```The quick brown fox jumped over the lazy dog's back```

A proximity search in the form of: ```jumped w/2 quick``` would match the above because 
 - there are no more than two tokens between
 - order was not required

Whereas ```jumped wo/2 quick``` would *not* match because order was required.

Proximity clauses can be chained together and are evaluated right-to-left.  For example:

```quick w/2 jumped w/4 back``` is evaluated as if it were written as ```quick w/2 (jumped w/4 back)```.

Additionally, phrase proximity is supported.  For example:

```"quick brown fox" w/3 "lazy dog's back"```

Proximity operators take the highest precedence, so when combined with other boolean operators they are evaluated first.  For example:

```quick and "brown fox" w/3 "lazy dog's" and back``` is evaluated as if it were written as: ```quick and ("brown fox" w/3 "lazy dog's") and back```

Proximity clauses can be limited to specific fields as well:  ``title:catcher w/2 title:rye``.  Note that mixeding fieldnames in a proximity chain is non-sensical and will produce a parse error.


 