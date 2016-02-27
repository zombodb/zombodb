CREATE TABLE issue_85 (
  id serial8 NOT NULL PRIMARY KEY,
  data json
);
CREATE INDEX idxissue_85 ON issue_85 USING zombodb(zdb('issue_85', ctid), zdb_to_jsonb(issue_85)) WITH (url='http://localhost:9200/');

INSERT INTO issue_85(data) values ('{"title": "this is the title"}');
SELECT * FROM zdb_tally('issue_85', 'data.title', '^.*', '', 5000, 'count');
DROP TABLE issue_85;