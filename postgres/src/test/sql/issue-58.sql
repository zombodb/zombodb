CREATE TABLE a (
  id SERIAL8 PRIMARY KEY
);
CREATE TABLE b (
  id SERIAL8 PRIMARY KEY
);
CREATE TABLE c (
  id SERIAL8 PRIMARY KEY
);

CREATE INDEX idxc ON c USING zombodb(zdb('c', ctid), zdb(C)) WITH (url='http://localhost:9200/');
CREATE OR REPLACE FUNCTION public.issue_58_func(table_name REGCLASS, ctid tid)
  RETURNS tid
LANGUAGE C
IMMUTABLE STRICT
AS '$libdir/plugins/zombodb', 'zdb_table_ref_and_tid';

CREATE INDEX idxc_shadow ON c USING zombodb(issue_58_func('c', ctid), zdb(C)) WITH (shadow='idxc');

CREATE VIEW issue_58_view AS
  SELECT
    a.id                       AS a_id,
    b.id                       AS b_id,
    c.id                       AS c_id,
    issue_58_func('c', c.ctid) AS zdb
  FROM a, b, C;

set enable_seqscan to off;
set ENABLE_BITMAPSCAN to off;
explain (costs off) select * from issue_58_view where zdb==>'';
select assert(zdb_determine_index('issue_58_view')::regclass, 'idxc_shadow'::regclass, 'Found correct index');

DROP VIEW issue_58_view;
DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
DROP FUNCTION issue_58_func( REGCLASS, tid );