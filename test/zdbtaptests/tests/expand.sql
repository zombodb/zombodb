-- USAGE: $ pg_prove -d zdbtaptests -U postgres src/test/zdbtaptests/tests/expand.sql

-- Start transaction and plan the tests.
BEGIN;
SELECT plan(159);

-- Run the tests.
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" ))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" ))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" ))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[7]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes');
--**********************************************************************************************************************



--test #expand with #filter
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 4, 5, 6, 8, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 6, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 4, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 4, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 4, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 4, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 4, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 6, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 4, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 6, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 6, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 4, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 4, 5, 6, 8]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, red & snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, red & snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 5, 6, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 4, 5, 8]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, red & snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes, filter data:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes, filter data:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes, filter var:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 4, 5, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes, filter var:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 5, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand data, yellow & red & snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand var, yellow & red & snakes, filter vol:FALSE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2, 5, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes, filter vol:TRUE');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[4, 5]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', 'expand vol, yellow & red & snakes, filter vol:FALSE');
--**********************************************************************************************************************



-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
