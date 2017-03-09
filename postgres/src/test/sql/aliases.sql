CREATE TABLE aliases (id serial8 NOT NULL PRIMARY KEY, title text);
CREATE INDEX idxaliases ON aliases USING zombodb (zdb(aliases), zdb_to_json(aliases)) WITH (url='http://localhost:9200/', alias='the_alias');
INSERT INTO aliases VALUES (DEFAULT, 'one');
INSERT INTO aliases VALUES (DEFAULT, 'two');
INSERT INTO aliases VALUES (DEFAULT, 'three');
SELECT * FROM aliases WHERE zdb(aliases) ==> '' ORDER BY id;
SELECT zdb_estimate_count('aliases', '');
DROP TABLE aliases;