CREATE TABLE test_drop ();
CREATE INDEX idxtest_drop ON test_drop USING zombodb ((test_drop));
DROP INDEX idxtest_drop;
select * from (select json_array_elements(zdb.request('idxevents', '/_cat/indices?format=json')::json)->>'index' as index) x where index = 'contrib_regression.public.test_drop.idxtest_drop';
DROP TABLE test_drop;

CREATE TABLE test_drop ();
CREATE INDEX idxtest_drop ON test_drop USING zombodb ((test_drop));
DROP TABLE test_drop;
select * from (select json_array_elements(zdb.request('idxevents', '/_cat/indices?format=json')::json)->>'index' as index) x where index = 'contrib_regression.public.test_drop.idxtest_drop';


CREATE SCHEMA test_schema;
CREATE TABLE test_schema.test_drop ();
CREATE INDEX idxtest_drop ON test_schema.test_drop USING zombodb ((test_drop));
DROP SCHEMA test_schema cascade;
select * from (select json_array_elements(zdb.request('idxevents', '/_cat/indices?format=json')::json)->>'index' as index) x where index = 'contrib_regression.test_schema.test_drop.idxtest_drop';
