CREATE TABLE issue311 (
  id serial8 not null primary key,
  data json
);

SELECT zdb_define_mapping('issue311', 'data', '{
            "type" : "nested",
            "include_in_all" : false,
            "properties" : {
              "offset" : {
                "type" : "long"
              },
              "subjects" : {
                "type" : "keyword",
                "ignore_above" : 10921
              },
              "tags" : {
                "type" : "keyword",
                "ignore_above" : 10921
              },
              "terms" : {
                "type" : "keyword",
                "ignore_above" : 10921
              },
              "text" : {
                "type" : "text",
                "analyzer" : "fulltext",
                "include_in_all" : false
              },
              "zdb_always_exists" : {
                "type" : "boolean",
                "null_value" : true
              }
            }
          }');
CREATE INDEX idxissue311 ON issue311 USING zombodb (zdb('issue311', ctid), zdb(issue311)) WITH (url='localhost:9200/');
INSERT INTO issue311 (data) VALUES ('[{"tags":["a", "b"], "text":"this is a test"}]');

SELECT id FROM issue311 WHERE zdb('issue311', ctid) ==> 'data.tags:a WITH data.text:"a test"';

DROP TABLE issue311 CASCADE ;