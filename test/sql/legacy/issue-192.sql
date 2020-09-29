CREATE TABLE issue_192 (
  id serial8 NOT NULL PRIMARY KEY,
  tag varchar(80)
);

CREATE INDEX idxissue_192 ON issue_192 USING zombodb (zdb('issue_192', ctid), zdb(issue_192)) WITH (url='http://localhost:9200/');

INSERT INTO issue_192 (tag) VALUES ('test tag');
UPDATE issue_192 SET id = id WHERE id = 1;
REINDEX INDEX idxissue_192;
VACUUM issue_192;

DROP TABLE issue_192 CASCADE;