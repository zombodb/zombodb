CREATE TABLE issue881 (id serial8, t varchar);
INSERT INTO issue881 (t) values ('test');
CREATE INDEX idxissue881 ON issue881 USING zombodb ((issue881.*));
CREATE INDEX idxissue881_shadow ON issue881 USING zombodb ((issue881.*)) WITH (shadow='true');

SELECT zdb.count('issue881', '');
SELECT * FROM zdb.terms('issue881', 't', '');

BEGIN;
UPDATE issue881 SET id = id;
UPDATE issue881 SET t = 'this is the new value';
COMMIT;

SELECT zdb.count('issue881', '');
SELECT * FROM zdb.terms('issue881', 't', '');

DROP TABLE issue881;