SET datestyle TO 'iso, mdy';
CREATE TABLE multifield AS SELECT * FROM so_users ORDER BY id LIMIT 10;
ALTER TABLE multifield ADD PRIMARY KEY (id);
SELECT zdb_define_mapping('multifield', 'display_name', '{
    "type": "string", "analyzer": "phrase",
    "fields": {
        "raw":   { "type": "string", "index": "not_analyzed" }
    }
}');
CREATE INDEX idxmultifield ON multifield USING zombodb (zdb(multifield), zdb_to_json(multifield)) WITH (url='http://localhost:9200/');

SELECT display_name FROM multifield WHERE zdb(multifield) ==> 'display_name:robert';

SELECT display_name FROM multifield WHERE zdb(multifield) ==> 'display_name.raw:robert';
SELECT display_name FROM multifield WHERE zdb(multifield) ==> 'display_name.raw:"Robert Cartaino"';

DROP TABLE multifield;