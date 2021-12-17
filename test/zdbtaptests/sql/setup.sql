-- USAGE: $ psql -d postgres -U postgres -f sql/setup.sql

CREATE DATABASE zdbtaptests;
\c zdbtaptests 
\set zombodb_url '''http://127.0.0.1:9200/'''


CREATE EXTENSION pgtap;
CREATE EXTENSION hstore;
CREATE EXTENSION zombodb;

CREATE SCHEMA unit_tests;


--**********************************************************************************************************************
CREATE TABLE unit_tests.data(
  pk_data BIGINT
  , data_bigint_1 BIGINT
  , data_bigint_expand_group BIGINT
  , data_bigint_array_1 BIGINT[]
  , data_bigint_array_2 BIGINT[]
  , data_boolean BOOLEAN
  , data_char_1 CHAR(2)
  , data_char_2 CHAR(2)
  , data_char_array_1 CHAR(2)[]
  , data_char_array_2 CHAR(2)[]
  , data_date_1 DATE
  , data_date_2 DATE
  , data_date_array_1 DATE[]
  , data_date_array_2 DATE[]
  , data_full_text zdb.fulltext
  , data_full_text_shingles zdb.fulltext_with_shingles
  , data_int_1 INT
  , data_int_2 INT
  , data_int_array_1 INT[]
  , data_int_array_2 INT[]
  , data_json JSON
  , data_phrase_1 zdb.phrase
  , data_phrase_2 zdb.phrase
  , data_phrase_array_1 zdb.phrase_array
  , data_phrase_array_2 zdb.phrase_array
  , data_text_1 TEXT
  , data_text_filter TEXT
  , data_text_array_1 TEXT[]
  , data_text_array_2 TEXT[]
  , data_timestamp TIMESTAMP
  , data_varchar_1 VARCHAR(25)
  , data_varchar_2 VARCHAR(25)
  , data_varchar_array_1 VARCHAR(25)[]
  , data_varchar_array_2 VARCHAR(25)[]
, CONSTRAINT idx_unit_tests_data_pkey PRIMARY KEY (pk_data)
);

CREATE INDEX es_unit_tests_data
  ON unit_tests.data USING zombodb (( DATA.*)) WITH (url=:zombodb_url, OPTIONS ='pk_data = <unit_tests.var.es_unit_tests_var>pk_var,pk_data = <unit_tests.vol.es_unit_tests_vol>pk_vol', shards=32);
--**********************************************************************************************************************


--**********************************************************************************************************************
CREATE TABLE unit_tests.var(
  pk_var BIGINT
  , var_bigint_1 BIGINT
  , var_bigint_expand_group BIGINT
  , var_bigint_array_1 BIGINT[]
  , var_bigint_array_2 BIGINT[]
  , var_boolean BOOLEAN
  , var_char_1 CHAR(2)
  , var_char_2 CHAR(2)
  , var_char_array_1 CHAR(2)[]
  , var_char_array_2 CHAR(2)[]
  , var_date_1 DATE
  , var_date_2 DATE
  , var_date_array_1 DATE[]
  , var_date_array_2 DATE[]
  , var_int_1 INT
  , var_int_2 INT
  , var_int_array_1 INT[]
  , var_int_array_2 INT[]
  , var_json JSON
  , var_phrase_1 zdb.phrase
  , var_phrase_2 zdb.phrase
  , var_phrase_array_1 zdb.phrase_array
  , var_phrase_array_2 zdb.phrase_array
  , var_text_1 TEXT
  , var_text_filter TEXT
  , var_text_array_1 TEXT[]
  , var_text_array_2 TEXT[]
  , var_timestamp TIMESTAMP
  , var_varchar_1 VARCHAR(25)
  , var_varchar_2 VARCHAR(25)
  , var_varchar_array_1 VARCHAR(25)[]
  , var_varchar_array_2 VARCHAR(25)[]
, CONSTRAINT idx_unit_tests_var_pkey PRIMARY KEY (pk_var)
);

CREATE INDEX es_unit_tests_var
  ON unit_tests.var USING zombodb ((var.*)) WITH (url=:zombodb_url, shards=32);
--**********************************************************************************************************************


--**********************************************************************************************************************
CREATE TABLE unit_tests.vol(
  pk_vol BIGINT
  , vol_bigint_1 BIGINT
  , vol_bigint_expand_group BIGINT
  , vol_bigint_array_1 BIGINT[]
  , vol_bigint_array_2 BIGINT[]
  , vol_boolean BOOLEAN
  , vol_char_1 CHAR(2)
  , vol_char_2 CHAR(2)
  , vol_char_array_1 CHAR(2)[]
  , vol_char_array_2 CHAR(2)[]
  , vol_date_1 DATE
  , vol_date_2 DATE
  , vol_date_array_1 DATE[]
  , vol_date_array_2 DATE[]
  , vol_int_1 INT
  , vol_int_2 INT
  , vol_int_array_1 INT[]
  , vol_int_array_2 INT[]
  , vol_json JSON
  , vol_phrase_1 zdb.phrase
  , vol_phrase_2 zdb.phrase
  , vol_phrase_array_1 zdb.phrase_array
  , vol_phrase_array_2 zdb.phrase_array
  , vol_text_1 TEXT
  , vol_text_filter TEXT
  , vol_text_array_1 TEXT[]
  , vol_text_array_2 TEXT[]
  , vol_timestamp TIMESTAMP
  , vol_varchar_1 VARCHAR(25)
  , vol_varchar_2 VARCHAR(25)
  , vol_varchar_array_1 VARCHAR(25)[]
  , vol_varchar_array_2 VARCHAR(25)[]
, CONSTRAINT idx_unit_tests_vol_pkey PRIMARY KEY (pk_vol)
);

CREATE INDEX es_unit_tests_vol
  ON unit_tests.vol USING zombodb ((vol.*)) WITH (url=:zombodb_url, shards=32);
--**********************************************************************************************************************

-- TODO: RULES/TRIGGERS
--VIEW
--**********************************************************************************************************************
CREATE VIEW unit_tests.consolidated_record_view AS
  SELECT
    pk_data,
    pk_var,
    pk_vol
    , data_bigint_1
    , data_bigint_expand_group
    , data_bigint_array_1
    , data_bigint_array_2
    , data_boolean
    , data_char_1
    , data_char_2
    , data_char_array_1
    , data_char_array_2
    , data_date_1
    , data_date_2
    , data_date_array_1
    , data_date_array_2
    , data_full_text
    , data_full_text_shingles
    , data_int_1
    , data_int_2
    , data_int_array_1
    , data_int_array_2
    , data_json
    , data_phrase_1
    , data_phrase_2
    , data_phrase_array_1
    , data_phrase_array_2
    , data_text_1
    , data_text_filter
    , data_text_array_1
    , data_text_array_2
    , data_timestamp
    , data_varchar_1
    , data_varchar_2
    , data_varchar_array_1
    , data_varchar_array_2
    , var_bigint_1
    , var_bigint_expand_group
    , var_bigint_array_1
    , var_bigint_array_2
    , var_boolean
    , var_char_1
    , var_char_2
    , var_char_array_1
    , var_char_array_2
    , var_date_1
    , var_date_2
    , var_date_array_1
    , var_date_array_2
    , var_int_1
    , var_int_2
    , var_int_array_1
    , var_int_array_2
    , var_json
    , var_phrase_1
    , var_phrase_2
    , var_phrase_array_1
    , var_phrase_array_2
    , var_text_1
    , var_text_filter
    , var_text_array_1
    , var_text_array_2
    , var_timestamp
    , var_varchar_1
    , var_varchar_2
    , var_varchar_array_1
    , var_varchar_array_2
    , vol_bigint_1
    , vol_bigint_expand_group
    , vol_bigint_array_1
    , vol_bigint_array_2
    , vol_boolean
    , vol_char_1
    , vol_char_2
    , vol_char_array_1
    , vol_char_array_2
    , vol_date_1
    , vol_date_2
    , vol_date_array_1
    , vol_date_array_2
    , vol_int_1
    , vol_int_2
    , vol_int_array_1
    , vol_int_array_2
    , vol_json
    , vol_phrase_1
    , vol_phrase_2
    , vol_phrase_array_1
    , vol_phrase_array_2
    , vol_text_1
    , vol_text_filter
    , vol_text_array_1
    , vol_text_array_2
    , vol_timestamp
    , vol_varchar_1
    , vol_varchar_2
    , vol_varchar_array_1
    , vol_varchar_array_2
    , data AS zdb
    , data.ctid as data_ctid
  FROM unit_tests.data
  LEFT JOIN unit_tests.var ON data.pk_data = var.pk_var
  LEFT JOIN unit_tests.vol ON data.pk_data = vol.pk_vol;
--**********************************************************************************************************************


--LOAD DATA
--**********************************************************************************************************************
\copy unit_tests.data FROM 'raw_data/data.csv' WITH (FORMAT CSV, DELIMITER ',', HEADER);
\copy unit_tests.var FROM 'raw_data/var.csv' WITH (FORMAT CSV, DELIMITER ',', HEADER);
\copy unit_tests.vol FROM 'raw_data/vol.csv' WITH (FORMAT CSV, DELIMITER ',', HEADER);
--**********************************************************************************************************************


--DO SOME HOUSE KEEPING
--**********************************************************************************************************************
VACUUM ANALYZE unit_tests.data;
VACUUM ANALYZE unit_tests.var;
VACUUM ANALYZE unit_tests.vol;


-- same as above, but with similarly-named primary key columns
--**********************************************************************************************************************

CREATE TABLE unit_tests.data_same AS SELECT * FROM unit_tests.data;
CREATE TABLE unit_tests.var_same AS SELECT * FROM unit_tests.var;
CREATE TABLE unit_tests.vol_same AS SELECT * FROM unit_tests.vol;

ALTER TABLE unit_tests.data_same RENAME COLUMN pk_data TO id;
ALTER TABLE unit_tests.var_same RENAME COLUMN pk_var TO id;
ALTER TABLE unit_tests.vol_same RENAME COLUMN pk_vol TO id;

ALTER TABLE unit_tests.data_same ADD PRIMARY KEY (id);
ALTER TABLE unit_tests.var_same ADD PRIMARY KEY (id);
ALTER TABLE unit_tests.vol_same ADD PRIMARY KEY (id);

CREATE INDEX es_unit_tests_data_same
  ON unit_tests.data_same USING zombodb ((data_same.*)) WITH (url=:zombodb_url, OPTIONS ='id = <unit_tests.var_same.es_unit_tests_var_same>id, id = <unit_tests.vol_same.es_unit_tests_vol_same>id', shards=32);
CREATE INDEX es_unit_tests_var_same
  ON unit_tests.var_same USING zombodb ((var_same.*)) WITH (url=:zombodb_url, shards=32);
CREATE INDEX es_unit_tests_vol_same
  ON unit_tests.vol_same USING zombodb ((vol_same.*)) WITH (url=:zombodb_url, shards=32);

CREATE VIEW unit_tests.consolidated_record_view_same AS
  SELECT data_same.id
    , data_bigint_1
    , data_bigint_expand_group
    , data_bigint_array_1
    , data_bigint_array_2
    , data_boolean
    , data_char_1
    , data_char_2
    , data_char_array_1
    , data_char_array_2
    , data_date_1
    , data_date_2
    , data_date_array_1
    , data_date_array_2
    , data_full_text
    , data_full_text_shingles
    , data_int_1
    , data_int_2
    , data_int_array_1
    , data_int_array_2
    , data_json
    , data_phrase_1
    , data_phrase_2
    , data_phrase_array_1
    , data_phrase_array_2
    , data_text_1
    , data_text_filter
    , data_text_array_1
    , data_text_array_2
    , data_timestamp
    , data_varchar_1
    , data_varchar_2
    , data_varchar_array_1
    , data_varchar_array_2
    , var_bigint_1
    , var_bigint_expand_group
    , var_bigint_array_1
    , var_bigint_array_2
    , var_boolean
    , var_char_1
    , var_char_2
    , var_char_array_1
    , var_char_array_2
    , var_date_1
    , var_date_2
    , var_date_array_1
    , var_date_array_2
    , var_int_1
    , var_int_2
    , var_int_array_1
    , var_int_array_2
    , var_json
    , var_phrase_1
    , var_phrase_2
    , var_phrase_array_1
    , var_phrase_array_2
    , var_text_1
    , var_text_filter
    , var_text_array_1
    , var_text_array_2
    , var_timestamp
    , var_varchar_1
    , var_varchar_2
    , var_varchar_array_1
    , var_varchar_array_2
    , vol_bigint_1
    , vol_bigint_expand_group
    , vol_bigint_array_1
    , vol_bigint_array_2
    , vol_boolean
    , vol_char_1
    , vol_char_2
    , vol_char_array_1
    , vol_char_array_2
    , vol_date_1
    , vol_date_2
    , vol_date_array_1
    , vol_date_array_2
    , vol_int_1
    , vol_int_2
    , vol_int_array_1
    , vol_int_array_2
    , vol_json
    , vol_phrase_1
    , vol_phrase_2
    , vol_phrase_array_1
    , vol_phrase_array_2
    , vol_text_1
    , vol_text_filter
    , vol_text_array_1
    , vol_text_array_2
    , vol_timestamp
    , vol_varchar_1
    , vol_varchar_2
    , vol_varchar_array_1
    , vol_varchar_array_2
    , data_same AS zdb
  FROM unit_tests.data_same
  LEFT JOIN unit_tests.var_same ON data_same.id = var_same.id
  LEFT JOIN unit_tests.vol_same ON data_same.id = vol_same.id;

--create case table and populate
CREATE TABLE unit_tests.case_name
(
  pk_cpm bigint NOT NULL,
  cpm_name text,
  CONSTRAINT idx_unit_tests_case_name PRIMARY KEY (pk_cpm)
);

CREATE INDEX es_unit_tests_case_name
  ON unit_tests.case_name USING zombodb ((case_name.*)) WITH (url=:zombodb_url, shards=32);

ALTER INDEX unit_tests.es_unit_tests_data set (options='pk_data = <unit_tests.var.es_unit_tests_var>pk_var,pk_data = <unit_tests.vol.es_unit_tests_vol>pk_vol,data_bigint_array_2=<unit_tests.case_name.es_unit_tests_case_name>pk_cpm');


INSERT INTO unit_tests.case_name(pk_cpm,cpm_name)
values(1,'Smith, Bob'),(2,'Smith, Mary'),(3,'Jones, Mike'),(4,'Jones, Harry'),(5,'Jones, Sally'),(6,'Jones, UhOh');

--create shadow index and view
CREATE OR REPLACE FUNCTION zdb_data_to_case(anyelement) RETURNS anyelement
    IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS '$libdir/zombodb', 'shadow_wrapper' COST 1;

CREATE INDEX es_idx_data_to_case_shadow ON unit_tests.data USING zombodb (zdb_data_to_case((data.*)))
WITH (shadow=true, options='case_data:(data_bigint_array_2=<unit_tests.case_name.es_unit_tests_case_name>fk_jur_to_cpm)');

CREATE OR REPLACE VIEW unit_tests.data_json_agg_view AS
  select data.*,
    ( SELECT json_agg(row_to_json(cpm.*)) AS json_agg FROM
      ( SELECT case_name.*
        FROM unit_tests.case_name WHERE case_name.pk_cpm = ANY(data.data_bigint_array_2)) cpm) AS case_data,
    zdb_data_to_case(data) AS zdb
  FROM unit_tests.data;

VACUUM ANALYZE unit_tests.data_same;
VACUUM ANALYZE unit_tests.var_same;
VACUUM ANALYZE unit_tests.vol_same;
ALTER DATABASE zdbtaptests SET zdb.enable_search_accelerator TO ON;
--**********************************************************************************************************************
