CREATE TABLE issue272 (
  id serial8 NOT NULL PRIMARY KEY,
  data json
);

SELECT zdb.define_field_mapping('issue272', 'data', '{
  "type": "nested",
  "include_in_parent": true,
  "properties": {
    "obj1": {
      "type": "nested",
      "include_in_parent": true,
      "properties": {
        "key1": {
          "type": "keyword",
          "ignore_above": 10921,
          "normalizer": "exact"
        },
        "key2": {
          "type": "keyword",
          "ignore_above": 10921,
          "normalizer": "exact"
        }
      }
    }
  }
}');

CREATE INDEX idxissue272 ON issue272 USING zombodb ((issue272.*));

INSERT INTO issue272 (data) VALUES ('{
  "top_key": 1,
  "obj1": [
    {
      "key1": "val1",
      "key2": "val1"
    },
    {
      "key1": "val2",
      "key2": "val2"
    },
    {
      "key1": "val1",
      "key2": "val2"
    }
  ],
  "not_nested_obj": {
    "bool_field": true,
    "num_field": 1
  }
}');

INSERT INTO issue272 (data) VALUES ('{
  "top_key": 2,
  "obj1": [
    {
      "key1": "val10",
      "key2": "val10"
    },
    {
      "key1": "val20",
      "key2": "val20"
    },
    {
      "key1": "val10",
      "key2": "val20"
    }
  ],
  "not_nested_obj": {
    "bool_field": true,
    "num_field": 2
  }
}');

-- should return id=1
SELECT *
FROM issue272
WHERE issue272 ==> 'data.obj1.key1=val1 WITH data.obj1.key2=val1';


-- should also return id=1
select * from issue272 where issue272 ==> 'data.obj1.key1:val1' and id = 1;


-- should return all values for data.obj1.key1
select * from zdb.tally('idxissue272', 'data.obj1.key1', '^.*', '', 5000, 'term');

-- should return id=1
SELECT * FROM issue272 WHERE issue272 ==> 'data.obj1.key1=val1 with data.obj1.key2=val1 with data.top_key=1';

-- should return "val1"
select * from zdb.tally('idxissue272', 'data.obj1.key1', true, '^.*', 'data.obj1.key1=val1 with data.obj1.key2=val1 with data.top_key=1', 5000, 'term');

-- should return "val1" and "val2"
select * from zdb.tally('idxissue272', 'data.obj1.key1', false, '^.*', 'data.obj1.key1=val1 with data.obj1.key2=val1 with data.top_key=1', 5000, 'term');

-- should return id=1 and id=2
SELECT id FROM issue272 WHERE issue272 ==> 'data.not_nested_obj.bool_field:true' order by id;

DROP TABLE issue272 CASCADE;