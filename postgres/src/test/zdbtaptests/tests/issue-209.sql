BEGIN;

SELECT plan(1);

DEALLOCATE ALL;
PREPARE expected_result AS SELECT upper(term) term FROM zdb_tally('unit_tests.data_json_agg_view', 'case_data.cpm_name', '0', '^jo.*', '', 5000, 'term'::zdb_tally_order);
PREPARE zdb_result      AS SELECT upper(term) term FROM zdb_tally('unit_tests.data_json_agg_view', 'cpm_name',           '0', '^jo.*', '', 5000, 'term'::zdb_tally_order);

SELECT set_eq('expected_result', 'zdb_result', 'issue-209');

SELECT * FROM finish();
ABORT;