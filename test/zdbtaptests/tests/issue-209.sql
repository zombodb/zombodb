BEGIN;

SELECT plan(2);

DEALLOCATE ALL;
PREPARE expected_result AS SELECT upper(term) term FROM zdb.tally('unit_tests.data_json_agg_view', 'case_data.cpm_name', '0', 'jo.*', '', 5000, 'term'::termsorderby);
PREPARE zdb_result      AS SELECT upper(term) term FROM zdb.tally('unit_tests.data_json_agg_view', 'cpm_name',           '0', 'jo.*', '', 5000, 'term'::termsorderby);

SELECT set_eq('expected_result', 'zdb_result', 'issue-209 (v3.1.13)');


DEALLOCATE ALL;
PREPARE expected_result AS select animal from (select upper(json_array_elements(data_json)->>'animal') animal, count(*) from unit_tests.data group by 1) x where animal ilike 'c%';
PREPARE zdb_result      AS SELECT upper(term) animal FROM zdb.tally('unit_tests.data', 'data_json.animal', '1', 'c.*', '', 5000, 'term'::termsorderby);

SELECT set_eq('expected_result', 'zdb_result', 'issue-209, second round (v3.1.14)');

SELECT * FROM finish();
ABORT;