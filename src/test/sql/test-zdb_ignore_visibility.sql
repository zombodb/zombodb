SET zdb.ignore_visibility TO off;
SELECT count(*) FROM events WHERE events ==> dsl.match_all();

SET zdb.ignore_visibility TO on;
SELECT count(*) FROM events WHERE events ==> dsl.match_all();