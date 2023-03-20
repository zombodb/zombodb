CREATE TABLE issue807 (
    id SERIAL8 NOT NULL PRIMARY KEY,
    name text NOT NULL,
    testdate date
);
CREATE INDEX idxissue807 ON issue807 USING zombodb ((issue807.*));

INSERT INTO issue807(name, testdate)
SELECT 'testrow'||generate_series(1, 100, 1),
       (NOW() - '1 day'::INTERVAL * ROUND(RANDOM() * 100))::date;

SELECT count(*)
FROM issue807
         INNER JOIN LATERAL
    zdb.highlight_document('issue807'::regclass,
                           to_json(issue807),
                           'testdate >= "2022-12-01"'::TEXT) AS highlights ON true;


drop table issue807 cascade;