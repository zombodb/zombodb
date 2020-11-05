SET datestyle TO 'iso, mdy';
CREATE TABLE multifield AS SELECT * FROM so_users ORDER BY id LIMIT 10;
ALTER TABLE multifield ADD PRIMARY KEY (id);
SELECT zdb.define_field_mapping('multifield', 'display_name', '{
    "type": "text", "analyzer": "phrase",
    "fields": {
        "raw": { "type": "keyword"}
    }
}');
CREATE INDEX idxmultifield ON multifield USING zombodb((multifield.*));

SELECT display_name FROM multifield WHERE multifield ==> 'display_name:robert';

SELECT display_name FROM multifield WHERE multifield ==> 'display_name.raw:robert';
SELECT display_name FROM multifield WHERE multifield ==> 'display_name.raw:"Robert Cartaino"';

DROP TABLE multifield;