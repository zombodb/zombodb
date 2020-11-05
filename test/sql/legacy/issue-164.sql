create schema test_expand;

create table test_expand.data(pk_data bigint, data_family_group bigint, data_first_name text, constraint idx_test_expand_data_pkey primary key (pk_data));

create table test_expand.var(pk_var bigint, var_family_group bigint, var_pets text, constraint idx_test_expand_var_pkey primary key (pk_var));

insert into test_expand.data(pk_data, data_family_group, data_first_name) values(1,1,'mark');
insert into test_expand.data(pk_data, data_family_group, data_first_name) values(2,1,'eric');
insert into test_expand.data(pk_data, data_family_group, data_first_name) values(3,NULL,'terry');


insert into test_expand.var(pk_var, var_family_group, var_pets) values(1,1,'dogs');
insert into test_expand.var(pk_var, var_family_group, var_pets) values(2,1,'cats');
insert into test_expand.var(pk_var, var_pets) values(3,'minions');

CREATE INDEX es_test_expand_var ON test_expand.var USING zombodb ((var.*))
	WITH (shards='3', replicas='0');

CREATE INDEX es_test_expand_data ON test_expand.data USING zombodb ((data.*))
	WITH (options='pk_data = <test_expand.var.es_test_expand_var>pk_var', shards='3', replicas='0');

CREATE OR REPLACE VIEW test_expand.consolidated_record_view AS  SELECT data.pk_data
	,data.data_family_group
	,data.data_first_name
	,var.var_family_group
	,var.var_pets
    ,data.ctid AS zdb
   FROM test_expand.data
     LEFT JOIN test_expand.var ON data.pk_data = var.pk_var;

SELECT * FROM test_expand.consolidated_record_view;

SELECT * FROM test_expand.consolidated_record_view where zdb==>'( (#expand<data_family_group=<this.index>data_family_group>( ( data_first_name = "MARK" )  )) )' ORDER BY pk_data;
SELECT * FROM test_expand.consolidated_record_view where zdb==>'( (#expand<var_family_group=<this.index>var_family_group>( ( data_first_name = "MARK" )  )) )' ORDER BY pk_data;

SELECT * FROM test_expand.consolidated_record_view where zdb==>'( (#expand<data_family_group=<this.index>data_family_group>( ( var_pets = "DOGS" )  )) )' ORDER BY pk_data;
SELECT * FROM test_expand.consolidated_record_view where zdb==>'( (#expand<var_family_group=<this.index>var_family_group>( ( var_pets = "DOGS" )  )) )' ORDER BY pk_data;
SELECT * FROM test_expand.consolidated_record_view where zdb==>'( (#expand<var_family_group=<this.index>var_family_group>( ( var_pets = "DOGS" AND data_first_name = "mark")  )) )' ORDER BY pk_data;

DROP SCHEMA test_expand CASCADE;