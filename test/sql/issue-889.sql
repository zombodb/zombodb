CREATE TABLE issue889
(
    id       SERIAL8 NOT NULL PRIMARY KEY,
    name     text    NOT NULL,
    testdate date,
    testval  varchar
);
CREATE INDEX idxissue889
    ON issue889 USING zombodb ((issue889.*)) WITH (field_lists='my_list=[testdate,testval]');

INSERT INTO issue889 (name, testdate, testval)
VALUES ('pretzels', '2024-04-01', '72');
INSERT INTO issue889 (name, testdate, testval)
VALUES ('cheese', '2024-04-02', '82');
INSERT INTO issue889 (name, testdate, testval)
VALUES ('cocktail weenies', '2024-04-01', 'none');
INSERT INTO issue889 (name, testdate, testval)
VALUES ('buffalo wings', '2024-04-01', '2024-04-09');

SELECT *
FROM issue889
WHERE issue889 ==> 'my_list="2024-04-02"'
ORDER BY id;


WITH matches as MATERIALIZED (SELECT *
                              FROM issue889
                              WHERE id = ANY ('{"1","2","3"}')),
     highlights AS MATERIALIZED (SELECT (zdb.highlight_document(
             'issue889'::regclass,
             json_build_object('name', name, 'testdate', testdate, 'testval', testval),
             'my_list="2024-04-02"'::TEXT)).*,
                                        id as primary_key
                                 FROM matches)
SELECT *
FROM highlights
ORDER BY "primary_key", "field_name", "position";

WITH matches as MATERIALIZED (SELECT *
                              FROM issue889
                              WHERE id = ANY ('{"1","2","3","4"}')),
     highlights AS MATERIALIZED (SELECT (zdb.highlight_document(
             'issue889'::regclass,
             json_build_object('name', name, 'testdate', testdate, 'testval', testval),
             'my_list="2024-04-02" OR my_list="2024-04-09" OR testdate="2024-04-01"')).*,
                                        id as primary_key
                                 FROM matches)
SELECT *
FROM highlights
ORDER BY "primary_key", "field_name", "position";

DROP TABLE issue889 CASCADE;
