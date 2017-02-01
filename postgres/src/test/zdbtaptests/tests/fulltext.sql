-- USAGE: $ pg_prove -d zombo_tests -U postgres tests/fulltext.sql

-- Start transaction and plan the tests.
BEGIN;
SELECT plan(8);

-- Run the tests.
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"chuck"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"chuck"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"chuck norris"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"chuck norris"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"norri?"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltest: norri?');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"norri?"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: norri?');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"norri*"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: norri*');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"norri*"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: norri*');
--**********************************************************************************************************************



-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
