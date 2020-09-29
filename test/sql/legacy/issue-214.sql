CREATE TABLE issue_214 (id SERIAL PRIMARY KEY, name TEXT);
CREATE INDEX idx_zdb_foo ON issue_214 USING zombodb(zdb('issue_214', ctid), zdb(issue_214)) WITH (url="http://localhost:9200/");
INSERT INTO issue_214 (name) VALUES ('a!a');

SELECT * FROM zdb_dump_query('issue_214', '_all:a\!*');
SELECT assert(zdb_dump_query('issue_214', '_all:a\!*'), zdb_dump_query('issue_214', '_all:"a!*"'), '_all: wildcard escaped v/s not');
SELECT * FROM issue_214 WHERE zdb('issue_214', ctid) ==> '_all:a\!*';


SELECT * FROM zdb_dump_query('issue_214', 'name:a\!*');
SELECT assert(zdb_dump_query('issue_214', 'name:a\!*'), zdb_dump_query('issue_214', 'name:"a!*"'), 'name: wildcard escaped v/s not');
SELECT * FROM issue_214 WHERE zdb('issue_214', ctid) ==> 'name:a\!*';

DROP TABLE issue_214;
