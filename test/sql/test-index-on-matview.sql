CREATE MATERIALIZED VIEW matview_test AS SELECT * FROM events WHERE events ==> 'beer OR wine OR cheese';
CREATE INDEX idxmatview_test ON matview_test USING zombodb ((matview_test));
SELECT count(*) FROM matview_test WHERE matview_test ==> 'beer';
REFRESH MATERIALIZED VIEW matview_test;
SELECT count(*) FROM matview_test WHERE matview_test ==> 'beer';
DROP MATERIALIZED VIEW matview_test CASCADE;