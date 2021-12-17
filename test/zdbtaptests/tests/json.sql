-- USAGE: $ pg_prove -d zdbtaptests -U postgres src/test/zdbtaptests/tests/json.sql

-- Start transaction and plan the tests.
BEGIN;
SELECT plan(12);

-- Run the tests.
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" AND data_json.animal = "cats"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs AND cats');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal:("dogs" & "cats")';
SELECT set_eq('expected_result', 'zdb_result', 'data animal (dogs & cats)');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" OR data_json.animal = "cats"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs OR cats');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal:("dogs", "cats")';
SELECT set_eq('expected_result', 'zdb_result', 'data animal (dogs, cats)');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" AND data_json.food = "pizza"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs AND pizza');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" WITH data_json.food = "pizza"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs WITH pizza');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" % data_json.food = "pizza"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs % pizza');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 6, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" AND data_json.food = "quiche"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs AND quiche');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" OR data_json.food = "quiche"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs OR quiche');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 6, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" WITH data_json.food = "quiche"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs WITH quiche');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 6, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_json.animal = "dogs" % data_json.food = "quiche"';
SELECT set_eq('expected_result', 'zdb_result', 'data animal dogs % quiche');
--**********************************************************************************************************************




-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
