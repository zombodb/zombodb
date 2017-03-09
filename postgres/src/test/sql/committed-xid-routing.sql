CREATE SCHEMA e_0010;
CREATE TABLE e_0010.tas_tbl
(
  pk_tas bigserial NOT NULL,
  tas_name text,
  stuff json,
  CONSTRAINT idx_e_0010_tas_tbll_pkey PRIMARY KEY (pk_tas)
);

CREATE INDEX es_e_0010_tas_tbl ON e_0010.tas_tbl USING zombodb (zdb(tas_tbl), zdb_to_json(tas_tbl.*))
WITH (url='http://localhost:9200/', replicas='0', shards='32');


INSERT INTO e_0010.tas_tbl(tas_name, stuff) VALUES('john doe', '[{"color":"blue"}]');
INSERT INTO e_0010.tas_tbl(tas_name, stuff) VALUES('jane doe', '[{"color":"pink"}]');
INSERT INTO e_0010.tas_tbl(tas_name, stuff) VALUES('bob smith', '[{"color":"blue"},{"color":"green"}]');


SELECT * FROM e_0010.tas_tbl;

SELECT * FROM e_0010.tas_tbl, json_array_elements(stuff) as elem WHERE elem->>'color' = 'blue';
SELECT * FROM zdb_estimate_count('e_0010.tas_tbl', 'stuff.color:"blue"');


UPDATE e_0010.tas_tbl
SET stuff = NULL
WHERE pk_tas = 3;


SELECT * FROM zdb_estimate_count('e_0010.tas_tbl', 'stuff.color:"blue"');

DROP SCHEMA e_0010 CASCADE ;