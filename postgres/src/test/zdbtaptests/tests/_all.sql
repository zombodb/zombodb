-- USAGE: $ pg_prove -d zdbtaptests -U postgres src/test/zdbtaptests/tests/_all.sql

-- Start transaction and plan the tests.
BEGIN;
SELECT plan(22);

-- Run the tests.
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 8]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'joe';
SELECT set_eq('expected_result', 'zdb_result', 'joe');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 4, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'tx';
SELECT set_eq('expected_result', 'zdb_result', 'tx');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'tn';
SELECT set_eq('expected_result', 'zdb_result', 'tn');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'joe tx';
SELECT set_eq('expected_result', 'zdb_result', 'joe tx');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[8]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'joe tn';
SELECT set_eq('expected_result', 'zdb_result', 'joe tn');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'greene';
SELECT set_eq('expected_result', 'zdb_result', 'greene');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'bananas';
SELECT set_eq('expected_result', 'zdb_result', 'bananas');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[8]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'greene bananas';
SELECT set_eq('expected_result', 'zdb_result', 'greene bananas');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[8]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'greene bananas nc';
SELECT set_eq('expected_result', 'zdb_result', 'greene bananas nc');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'cats';
SELECT set_eq('expected_result', 'zdb_result', 'cats');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'apples';
SELECT set_eq('expected_result', 'zdb_result', 'apples');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'cats apples';
SELECT set_eq('expected_result', 'zdb_result', 'cats apples');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 3, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'nats';
SELECT set_eq('expected_result', 'zdb_result', 'nats');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'cats apples nats';
SELECT set_eq('expected_result', 'zdb_result', 'cats apples nats');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'tangential';
SELECT set_eq('expected_result', 'zdb_result', 'tangential');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NULL';
SELECT set_eq('expected_result', 'zdb_result', 'NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'AA';
SELECT set_eq('expected_result', 'zdb_result', 'AA');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> '2';
SELECT set_eq('expected_result', 'zdb_result', '2');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> '22';
SELECT set_eq('expected_result', 'zdb_result', '22');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'orang*';
SELECT set_eq('expected_result', 'zdb_result', 'orang*');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 6, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'AB';
SELECT set_eq('expected_result', 'zdb_result', 'AB');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'orang* AB';
SELECT set_eq('expected_result', 'zdb_result', 'orang* AB');



-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
