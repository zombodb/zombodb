CREATE TABLE issue244_exact (
   id serial8,
   exact_field varchar
);
CREATE INDEX idxissue244 ON issue244_exact USING zombodb ( (issue244_exact.*) );
INSERT INTO issue244_exact(exact_field) VALUES ('\');
INSERT INTO issue244_exact(exact_field) VALUES ('foo');

SELECT * FROM issue244_exact WHERE issue244_exact ==> '((exact_field:"\\") or (exact_field:"foo")) or ((exact_field:''\\'') or (exact_field:''foo''))' order by id;

DROP TABLE issue244_exact;

CREATE TABLE issue244_phrase (
   id serial8,
   phrase_field zdb.phrase
);
CREATE INDEX idxissue244 ON issue244_phrase USING zombodb ( (issue244_phrase.*) );
INSERT INTO issue244_phrase(phrase_field) VALUES ('\');
INSERT INTO issue244_phrase(phrase_field) VALUES ('foo');

SELECT * FROM issue244_phrase WHERE issue244_phrase ==> '((phrase_field:"\\") or (phrase_field:"foo")) or ((phrase_field:''\\'') or (phrase_field:''foo''))' order by id;

DROP TABLE issue244_phrase;