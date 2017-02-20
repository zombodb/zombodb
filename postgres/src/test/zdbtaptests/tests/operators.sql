-- USAGE: $ pg_prove -d zdbtaptests -U postgres src/test/zdbtaptests/tests/operators.sql

-- Start transaction and plan the tests.
BEGIN;
SELECT plan(32);

-- Run the tests.
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data:[1, 2, 3]';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data:[1, 2, 3]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data:[[1, 2, 3]]';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data:[[1, 2, 3]]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data=[1, 2, 3]';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data=[1, 2, 3]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data=[[1, 2, 3]]';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data=[[1, 2, 3]]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data!=[1, 2, 3]';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data!=[1, 2, 3]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data!=[[1, 2, 3]]';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data!=[[1, 2, 3]]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data<>[1, 2, 3]';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data<>[1, 2, 3]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data<>[[1, 2, 3]]';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data!=[[1, 2, 3]]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data:[1, 2, 3]';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data:[1, 2, 3]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data:[[1, 2, 3]]';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data:[[1, 2, 3]]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data=[1, 2, 3]';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data=[1, 2, 3]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data=[[1, 2, 3]]';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data=[[1, 2, 3]]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data!=[1, 2, 3]';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data!=[1, 2, 3]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data!=[[1, 2, 3]]';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data!=[[1, 2, 3]]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data<>[1, 2, 3]';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data<>[1, 2, 3]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data<>[[1, 2, 3]]';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data!=[[1, 2, 3]]');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data > 3';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data > 3');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data >= 3';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data >= 3');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data < 3';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data < 3');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data <= 3';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data <= 3');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data > 3';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data > 3');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data >= 3';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data >= 3');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data < 3';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data < 3');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data <= 3';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data <= 3');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data: 1 /TO/ 10';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data: 1 /TO/ 10');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data: 1 /TO/ 10';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data: 1 /TO/ 10');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data= 1 /TO/ 10';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data= 1 /TO/ 10');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data= 1 /TO/ 10';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data= 1 /TO/ 10');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data!= 1 /TO/ 10';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data= 1 /TO/ 10');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data!= 1 /TO/ 10';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data= 1 /TO/ 10');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data<> 1 /TO/ 10';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data<> 1 /TO/ 10');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data<> 1 /TO/ 10';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data<> 1 /TO/ 10');
--**********************************************************************************************************************






-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;






-- :~ 	field contains terms matching a regular expression. Note that regular expression searches are always case sensitive.
-- :@ 	"more like this"
-- :@~ 	"fuzzy like this"