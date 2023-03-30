CREATE TABLE issue822 (
                                      id SERIAL8 NOT NULL PRIMARY KEY,
                                      name text NOT NULL,
                                      testdate date
);
CREATE INDEX es_idxwildcardeddatetest
    ON issue822 USING zombodb ((issue822.*));

INSERT INTO issue822(name, testdate)
SELECT 'testrow'||x, '2022-12-01'::date + (x||' days')::interval FROM generate_series(1, 1000) x;

-- should be a prefix query
select * from zdb.dump_query('issue822', '( testdate  = "2022-12-*" )');

SELECT * FROM issue822 WHERE issue822 ==> '( testdate  = "2022-12-*" )' order by id;
SELECT * FROM issue822 WHERE issue822 ==> '( testdate  = "2022-12*" )' order by id;

SELECT upper(term) term, count
FROM zdb.tally('issue822'::regclass,
               'name',
               'FALSE',
               '^.*',
               '( testdate  = "2022-12*" )'::zdbquery, 2147483647,
               'count'::termsorderby) order by 1, 2;
DROP TABLE issue822;