-- USAGE: $ pg_prove -d zdbtaptests -U postgres src/test/zdbtaptests/tests/null.sql

-- Start transaction and plan the tests.
BEGIN;
SELECT plan(12);

-- Run the tests.
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data:NULL';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data:NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'pk_data<>NULL';
SELECT set_eq('expected_result', 'zdb_result', 'pk_data<>NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data:NULL';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data:NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> '! pk_data:NULL';
SELECT set_eq('expected_result', 'zdb_result', '! pk_data:NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT pk_data<>NULL';
SELECT set_eq('expected_result', 'zdb_result', 'NOT pk_data<>NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> '! pk_data<>NULL';
SELECT set_eq('expected_result', 'zdb_result', '! pk_data<>NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS VALUES (7::BIGINT);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_bigint_1:NULL';
SELECT set_eq('expected_result', 'zdb_result', 'data_bigint_1:NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_bigint_1<>NULL';
SELECT set_eq('expected_result', 'zdb_result', 'data_bigint_1<>NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT data_bigint_1:NULL';
SELECT set_eq('expected_result', 'zdb_result', 'NOT data_bigint_1:NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> '! data_bigint_1:NULL';
SELECT set_eq('expected_result', 'zdb_result', '! data_bigint_1:NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS VALUES (7::BIGINT);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'NOT data_bigint_1<>NULL';
SELECT set_eq('expected_result', 'zdb_result', 'NOT data_bigint_1<>NULL');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS VALUES (7::BIGINT);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> '! data_bigint_1<>NULL';
SELECT set_eq('expected_result', 'zdb_result', '! data_bigint_1<>NULL');



-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
