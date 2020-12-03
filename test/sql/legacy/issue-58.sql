CREATE TABLE a (
  id SERIAL8 PRIMARY KEY
);
CREATE TABLE b (
  id SERIAL8 PRIMARY KEY
);
CREATE TABLE c (
  id SERIAL8 PRIMARY KEY
);

CREATE INDEX idxc ON c USING zombodb((c));

CREATE VIEW issue_58_view AS
  SELECT
    a.id                       AS a_id,
    b.id                       AS b_id,
    c.id                       AS c_id,
    c                          AS zdb
  FROM a, b, c;
COMMENT ON VIEW issue_58_view IS $$
    {
        "index": "public.idxc"
    }
$$;

set enable_seqscan to off;
set ENABLE_BITMAPSCAN to off;
explain (costs off) select * from issue_58_view where zdb==>'';
select assert(zdb.determine_index('issue_58_view')::regclass, 'idxc'::regclass, 'Found correct index');

DROP VIEW issue_58_view;
DROP TABLE a;
DROP TABLE b;
DROP TABLE c;
