BEGIN;

SELECT plan(1);

DEALLOCATE ALL;
PREPARE expected_result AS SELECT 1 UNION SELECT 2;
PREPARE zdb_result      AS SELECT pk_data FROM (select zdb.score(data_ctid), * from unit_tests.consolidated_record_view where zdb==> 'data_phrase_array_1:(mouse, mother)' order by zdb.score(data_ctid) desc) x;

SELECT set_eq('expected_result', 'zdb_result', 'issue-261');

SELECT * FROM finish();
ABORT;