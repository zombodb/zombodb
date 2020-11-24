CREATE TABLE issue_214 (id SERIAL PRIMARY KEY, name varchar);
CREATE INDEX idx_zdb_foo ON issue_214 USING zombodb((issue_214.*));
INSERT INTO issue_214 (name) VALUES ('a!a');

SELECT * FROM zdb.dump_query('idx_zdb_foo', '_all:a\!*');
SELECT assert(zdb.dump_query('idx_zdb_foo', '_all:a\!*'), zdb.dump_query('idx_zdb_foo', '_all:"a!*"'), '_all: wildcard escaped v/s not');
SELECT * FROM issue_214 WHERE issue_214 ==> '_all:a\!*';


SELECT * FROM zdb.dump_query('idx_zdb_foo', 'name:a\!*');
SELECT assert(zdb.dump_query('idx_zdb_foo', 'name:a\!*'), zdb.dump_query('idx_zdb_foo', 'name:"a!*"'), 'name: wildcard escaped v/s not');
SELECT * FROM issue_214 WHERE issue_214 ==> 'name:a\!*';

DROP TABLE issue_214;
