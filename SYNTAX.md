###### TODO:  (this document is incomplete)

# Query Syntax

The ```zombodb``` query syntax is modeled after standard SQL with many conveniences for text-search operations.

An example query might look like:

```
    beer wine cheese w/3 food
```

Which would find all documents that contain the words ```beer``` __and__ ```wine``` __and__ occurrences of ```cheese``` within 3 words of ```food```, regardless of the field (or fields) that contain each word.

The ```zombodb``` query syntax provides support for searching (in no particular order):

* words
* phrases
* out-of-order phrases
* fuzzy words
* proximity (of word or phrase or combinations)
* full boolean operators (AND, OR, NOT)
* value ranges
* wildcards (left, middle, and right truncation)
* scripted searching
* query expansion

## Boolean expressions and operator precedence

The supported set of boolean operators are the standard __NOT__, __AND__, and __OR__ operators.  If no operator is declared between terms, __AND__ is assumed.  Additionally, parenthetical groupings are allowed to form complex boolean expressions.

It is important to understand the operator precedence.  __NOT__ takes the highest priority, followed by __AND__, then finally __OR__.

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