CREATE TABLE cats (id integer, value text);
CREATE INDEX cats_zdb_idx ON cats USING zombodb ((cats.*)) WITH (url='http://localhost:9200/');

BEGIN;
INSERT INTO cats VALUES (1, 'foo');
UPDATE cats SET value = 'bar' WHERE id = 1;
DELETE FROM cats WHERE id = 1;
COMMIT;
DROP TABLE cats;