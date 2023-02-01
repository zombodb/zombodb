CREATE TABLE issue792 AS SELECT 'Vallejo has produced film posters for numerous fantasy and action movies, including Knightriders (1981), Q (1982), and Barbarian Queen (1985). He has also illustrated posters for comedies, notably National Lampoons Vacation (1983), European Vacation (1985), Nothing But Trouble (1991) and Aqua Teen Hunger Force Colon Movie Film for Theaters (2007), co-created with Bell.[8]
He created the 1978 Tarzan calendar.[citation needed] His sea serpent paintings hang in the queue of Loch Ness Monster, a rollercoaster at Busch Gardens Williamsburg.' AS t;

CREATE INDEX idxissue792 ON issue792 USING zombodb ((issue792.*));

SELECT * FROM issue792 WHERE issue792 ==> 't: ( "film" w/7 ("movies" w/7 "barbarian"))';

-- The term "film" is not within 7 of "barbarian" but should highlight as part of nested group
WITH highlights AS MATERIALIZED (SELECT (
    zdb.highlight_document('issue792'::regclass, json_build_object('t',t), 't: ( "film" w/7 ("movies" w/7 "barbarian"))'::TEXT)).* FROM issue792)
SELECT * FROM highlights order by position;

-- The term "film" is not within 7 of "barbarian" and "queen" but should highlight as part of nested group
WITH highlights AS MATERIALIZED (SELECT (
    zdb.highlight_document('issue792'::regclass, json_build_object('t',t), 't: ( "film" w/7 ("movies" w/7 ("barbarian", "queen")))'::TEXT)).* FROM issue792)
SELECT * FROM highlights order by position;

-- The term "movie" is within 6 of "barbarian" but not "queen" -- should not highlight "queen"
WITH highlights AS MATERIALIZED (SELECT (
    zdb.highlight_document('issue792'::regclass, json_build_object('t',t), 't: ( "film" w/7 ("movies" w/6 ("barbarian", "queen")))'::TEXT)).* FROM issue792)
SELECT * FROM highlights order by position;

-- Adjusted so that term "film" is within 25 of nested group and all three terms are highlighted
WITH highlights AS MATERIALIZED (SELECT (
    zdb.highlight_document('issue792'::regclass, json_build_object('t',t), 't: ( "film" w/25 ("movies" w/7 "barbarian"))'::TEXT)).* FROM issue792)
SELECT * FROM highlights order by position;

DROP TABLE issue792;