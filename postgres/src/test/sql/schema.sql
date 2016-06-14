CREATE SCHEMA test_schema;
CREATE TABLE test_schema.test_table (
  id    SERIAL8 PRIMARY KEY,
  title TEXT
);
CREATE INDEX idxtest_table ON test_schema.test_table USING zombodb(zdb('test_schema.test_table', ctid), zdb(test_table)) WITH (url='http://localhost:9200/');
SELECT * FROM zdb_estimate_count('test_schema.test_table', 'beer');

DROP SCHEMA test_schema CASCADE;
