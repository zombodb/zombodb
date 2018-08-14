CREATE ROLE issue274;
SET ROLE issue274;
SELECT 'test'::zdb.fulltext;
SELECT dsl.match_all();
DROP ROLE issue274;