BEGIN;

SELECT plan(1);

DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY [1, 3, 6, 10] :: BIGINT []);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view
                      WHERE zdb ==>
                            '
#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>(
        #expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>(
                data_char_1 = "AA" AND var_bigint_expand_group = 5
        )
        AND data_full_text:"*while"
)';

SELECT set_eq('expected_result', 'zdb_result', '#1: nested expansions');

SELECT * FROM finish();
ABORT;