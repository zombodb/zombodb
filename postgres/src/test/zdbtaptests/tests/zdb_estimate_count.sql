-- USAGE: $ pg_prove -d zdbtaptests -U postgres src/test/zdbtaptests/tests/zdb_estimate_count.sql

-- Start transaction and plan the tests.
BEGIN;
SELECT plan(19);

-- Run the tests.
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 4;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'var_bigint_expand_group = 5');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => var_bigint_expand_group = 5');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 4;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'pk_data:"*" AND var_bigint_expand_group = 5 ');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => pk_data:"*" AND var_bigint_expand_group = 5');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 4;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'pk_data: 1 /TO/ 10 AND var_bigint_expand_group = 5');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => pk_data: 1 /TO/ 10 AND var_bigint_expand_group = 5');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 4;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'pk_var: 1 /TO/ 10 AND var_bigint_expand_group = 5');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => pk_var: 1 /TO/ 10 AND var_bigint_expand_group = 5');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 4;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'pk_vol: 1 /TO/ 10 AND var_bigint_expand_group = 5');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => pk_vol: 1 /TO/ 10 AND var_bigint_expand_group = 5');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT COUNT(*)  FROM unit_tests.consolidated_record_view WHERE zdb ==> 'var_bigint_expand_group = 5';
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'var_bigint_expand_group = 5');
SELECT set_eq('expected_result', 'zdb_result', 'zdb vs zdb estimate => var_bigint_expand_group = 5');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 5;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'data_char_1: "ZZ"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => data_char_1: "ZZ"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 3;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'data_char_1: "ZZ" AND var_bigint_expand_group: "6"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => data_char_1: "ZZ" AND var_bigint_expand_group: "6"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 3;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'data_char_1: "ZZ" AND var_bigint_expand_group: "6"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => data_char_1: "ZZ" AND var_bigint_expand_group: "6"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 2;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'data_char_1: "ZZ" AND var_bigint_expand_group: "6" AND vol_bigint_expand_group: "9"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => data_char_1: "ZZ" AND var_bigint_expand_group: "6" AND vol_bigint_expand_group: "9"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 1;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'data_char_1: "ZZ" AND var_bigint_expand_group: "6" AND vol_bigint_expand_group: "8"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => data_char_1: "ZZ" AND var_bigint_expand_group: "6" AND vol_bigint_expand_group: "8"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 3;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'var_bigint_expand_group: "6"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => var_bigint_expand_group: "6"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 5;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'vol_bigint_expand_group: "9"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => vol_bigint_expand_group: "9"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 4;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'vol_bigint_expand_group: "8"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => vol_bigint_expand_group: "8"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 2;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'var_bigint_expand_group: "6" AND vol_bigint_expand_group: "9"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => var_bigint_expand_group: "6" AND vol_bigint_expand_group: "9"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 1;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'var_bigint_expand_group: "6" AND vol_bigint_expand_group: "8"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => var_bigint_expand_group: "6" AND vol_bigint_expand_group: "8"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 10;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'pk_data: 1 /TO/ 10');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => pk_data: 1 /TO/ 10');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 4;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'data_bigint_expand_group: "1"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => data_bigint_expand_group: "1"');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT 4;
PREPARE zdb_result AS SELECT * FROM zdb_estimate_count('unit_tests.consolidated_record_view', 'pk_data: 1 /TO/ 10 AND data_bigint_expand_group: "1"');
SELECT set_eq('expected_result', 'zdb_result', 'zdb estimate => pk_data: 1 /TO/ 10 AND data_bigint_expand_group: "1"');
--**********************************************************************************************************************



-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
