DROP ROLE IF EXISTS issue274;
CREATE ROLE issue274 WITH CREATEROLE;
SET ROLE issue274;
SELECT 'test'::zdb.fulltext;
SELECT dsl.match_all();
