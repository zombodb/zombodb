BEGIN;
SELECT plan(159);
DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" ))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" ))) )';
SELECT set_eq('expected_result', 'zdb_result', '1');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" ))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" ))) )';
SELECT set_eq('expected_result', 'zdb_result', '2');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" ))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" ))) )';
SELECT set_eq('expected_result', 'zdb_result', '3');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
SELECT set_eq('expected_result', 'zdb_result', '4');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
SELECT set_eq('expected_result', 'zdb_result', '5');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow"))) )';
SELECT set_eq('expected_result', 'zdb_result', '6');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '7');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '8');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( (vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '9');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
SELECT set_eq('expected_result', 'zdb_result', '10');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
SELECT set_eq('expected_result', 'zdb_result', '11');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red"))) )';
SELECT set_eq('expected_result', 'zdb_result', '12');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '13');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '14');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '15');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '16');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '17');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( (data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '18');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '19');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '20');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes"))) )';
SELECT set_eq('expected_result', 'zdb_result', '21');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '22');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '23');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '24');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '25');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '26');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '27');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '28');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '29');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '30');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '31');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '32');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json = NULL))) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json = NULL))) )';
SELECT set_eq('expected_result', 'zdb_result', '33');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '34');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '35');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '36');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '37');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '38');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '39');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '40');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '41');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '42');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '43');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '44');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '45');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '46');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '47');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '48');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '49');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '50');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '51');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '52');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '53');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '54');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '55');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '56');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '57');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '58');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '59');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '60');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '61');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '62');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '63');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '64');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '65');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '66');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '67');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '68');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '69');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '70');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '71');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '72');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '73');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '74');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '75');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '76');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '77');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '78');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '79');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '80');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '81');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '82');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '83');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '84');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '85');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '86');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '87');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '88');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '89');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '90');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '91');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '92');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '93');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '94');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '95');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '96');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '97');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '98');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '99');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '100');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '101');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '102');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '103');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '104');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '105');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '106');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '107');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '108');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '109');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '110');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '111');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '112');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '113');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '114');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '115');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '116');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '117');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '118');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '119');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '120');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '121');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '122');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '123');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '124');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '125');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '126');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '127');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '128');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '129');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '130');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '131');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '132');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '133');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '134');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '135');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '136');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '137');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '138');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '139');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '140');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '141');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '142');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '143');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '144');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '145');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '146');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(data_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '147');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '148');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '149');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '150');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '151');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '152');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(var_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '153');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '154');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<data_bigint_expand_group=<this.index>data_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '155');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '156');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<var_bigint_expand_group=<this.index>var_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '157');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:TRUE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '158');
--***

DEALLOCATE ALL;
PREPARE expected_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
PREPARE zdb_result      AS SELECT id FROM unit_tests.consolidated_record_view_same where zdb==>'( (#expand<vol_bigint_expand_group=<this.index>vol_bigint_expand_group>( ( var_text_1 = "yellow" AND data_text_1 = "red" AND vol_json.animal = "snakes") #filter(vol_boolean:FALSE)) ) )';
SELECT set_eq('expected_result', 'zdb_result', '159');
--***

SELECT * FROM finish();
ROLLBACK;
