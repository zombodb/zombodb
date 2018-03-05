CREATE TABLE IF NOT EXISTS testlcase (
  uri   TEXT NOT NULL,
  terms TEXT [],
  subjs JSON,
  PRIMARY KEY (uri)
);

INSERT INTO testlcase VALUES
  ('u1', '{t1,t2}', '[
    {
      "sid": "s1",
      "val": "foo"
    },
    {
      "sid": "s2",
      "val": "bar"
    }
  ]' :: JSON),
  ('U2', '{T1,T2}', '[
    {
      "sid": "S1",
      "val": "Foo"
    },
    {
      "sid": "S2",
      "val": "Bar"
    }
  ]' :: JSON);

CREATE INDEX idx_zdb_testlcase
  ON testlcase
  USING zombodb(zdb('testlcase', testlcase.ctid), zdb(testlcase))
WITH (url = 'http://localhost:9200/');

SELECT zdb_define_mapping('testlcase', 'terms', '{
          "store": false,
          "type": "keyword",
          "index_options": "docs",
          "include_in_all": "false"
        }');

SELECT zdb_define_mapping('testlcase', 'subjs', '{
            "type" : "nested",
            "include_in_all" : true,
            "properties" : {
              "sid" : {
                "type" : "keyword",
                "ignore_above" : 10921
              },
              "val" : {
                "type" : "keyword",
                "ignore_above" : 10921,
                "normalizer" : "exact"
              },
              "zdb_always_exists" : {
                "type" : "boolean",
                "null_value" : true
              }
            }
        }');

REINDEX INDEX idx_zdb_testlcase;

SELECT *
FROM testlcase
WHERE zdb('testlcase', testlcase.ctid) ==> 'terms:("T1")';

SELECT zdb_dump_query('testlcase', 'terms:("T1")');


SELECT *
FROM testlcase
WHERE zdb('testlcase', testlcase.ctid) ==> 'subjs.sid:("S1")';

SELECT zdb_dump_query('testlcase', 'subjs.sid:("S1")');

DROP TABLE testlcase CASCADE;