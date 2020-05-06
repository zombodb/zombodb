CREATE TABLE es_only (
    id serial8 not null primary key,
    first_name varchar(25),
    last_name varchar(64)
);
SELECT zdb. define_field_mapping('es_only', 'first_name', '{"type":"keyword", "copy_to":"combined_names"}');
SELECT zdb. define_field_mapping('es_only', 'last_name', '{"type":"keyword", "copy_to":"combined_names"}');
SELECT zdb.define_es_only_field('es_only', 'combined_names', '{"type":"text", "analyzer":"standard", "fielddata":true}');

CREATE INDEX idxes_only ON es_only USING zombodb ((es_only.*));
INSERT INTO es_only (first_name, last_name) VALUES ('Bob', 'Dole');
INSERT INTO es_only (first_name, last_name) VALUES ('Johnny', 'Carson');

SELECT * FROM es_only WHERE es_only ==> 'combined_names:Bob combined_names:Carson' ORDER BY id;
SELECT * FROM zdb.terms('idxes_only', 'combined_names', match_all()) ORDER BY term DESC;
DROP TABLE es_only;