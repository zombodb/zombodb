CREATE TABLE a (
  a_id SERIAL8 PRIMARY KEY
);
CREATE TABLE b (
  b_id SERIAL8 PRIMARY KEY
);

CREATE INDEX idxa on a USING zombodb(zdb('a', ctid), zdb(a)) WITH (url='http://localhost:9200/', options='a_id=<b.idxb>b_id');
CREATE INDEX idxb ON b USING zombodb(zdb('b', ctid), zdb(b)) WITH (url='http://localhost:9200/');

SELECT * FROM zdb_arbitrary_aggregate('a', $$ #tally(a_id, '^.*', 100, 'term', #tally(b_id, '^.*', 100, 'term')) $$, '');


DROP TABLE a;
DROP TABLE b;
