CREATE TABLE issue304 (
  id serial8 not null primary key,
  data json
);

CREATE INDEX idxissue304 ON issue304 USING zombodb (zdb('issue304', ctid), zdb(issue304)) WITH (url='localhost:9200/');
INSERT INTO issue304 (data) VALUES ('[{"tags":["a", "b"], "text":"test"}]');

SELECT id FROM issue304 WHERE zdb('issue304', ctid) ==> 'not data.tags:a WITH data.text:test';

DROP TABLE issue304 CASCADE ;