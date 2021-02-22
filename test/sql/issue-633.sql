CREATE TABLE issue633 (id serial8 not null primary key, data jsonb);
INSERT INTO issue633 (data) VALUES ('{"property": "this is the value"}');
CREATE INDEX idxissue633 ON issue633 USING zombodb ((issue633.*));

SELECT * FROM issue633 WHERE issue633 ==> 'data.property = "this *"';

DROP TABLE issue633;