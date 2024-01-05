CREATE TABLE issue884
(
    id       SERIAL8 NOT NULL PRIMARY KEY,
    name     text    NOT NULL,
    testdate date
);
CREATE INDEX es_idxhighlighteddatetest
    ON issue884 USING zombodb ((issue884.*));

INSERT INTO issue884(name, testdate)
SELECT 'testrow ' || x, '2024-01-05'::date - (x || ' days')::interval from generate_series(1, 100) x;

-- date field
SELECT issue884.*
FROM issue884
WHERE issue884 ==> 'testdate = "2023-12-10" /TO/ "2023-12-11"';

SELECT issue884.*, id AS primary_key
FROM issue884
         INNER JOIN LATERAL
    zdb.highlight_document('issue884'::regclass,
                           to_json(issue884),
                           'testdate = "2023-12-10" /TO/ "2023-12-11"'::TEXT) ON TRUE
ORDER BY "primary_key", "field_name", "position";


-- id field
SELECT issue884.*
FROM issue884
WHERE issue884 ==> 'id = 27 /TO/ 31';

SELECT issue884.*, id AS primary_key
FROM issue884
         INNER JOIN LATERAL
    zdb.highlight_document('issue884'::regclass,
                           to_json(issue884),
                           'id = 27 /TO/ 31'::TEXT) ON TRUE
ORDER BY "primary_key", "field_name", "position";

DROP TABLE issue884;