SET datestyle TO 'iso, mdy';

CREATE MATERIALIZED VIEW matview_test AS SELECT * FROM products;
CREATE INDEX idxmatview_test ON matview_test USING zombodb (zdb('matview_test', ctid), zdb(matview_test)) WITH (url='http://localhost:9200/');
SELECT * FROM zdb_estimate_count('matview_test', 'id:*');
SELECT * FROM products WHERE zdb('matview_test', ctid) ==> 'box' ORDER BY id;
DROP MATERIALIZED VIEW matview_test;