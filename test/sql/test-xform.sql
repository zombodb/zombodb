CREATE TABLE xform_test AS SELECT * FROM events WHERE events ==> 'beer OR wine OR cheese';

CREATE VIEW xform_test_view AS
    SELECT xform_test.*,
           users.url,
           users.login,
           users.avatar_url,
           users.gravatar_id,
           users.display_login
      FROM xform_test
      LEFT JOIN users ON xform_test.user_id = users.id;

CREATE OR REPLACE FUNCTION xform(data xform_test) RETURNS xform_test_view IMMUTABLE STRICT LANGUAGE sql AS $$
    SELECT *
      FROM xform_test_view
     WHERE id = data.id;
$$;

CREATE INDEX idxxform_test ON xform_test USING zombodb (xform(xform_test));

-- this should be a sequential scan
set enable_indexscan to off;
set enable_bitmapscan to off;
set enable_seqscan to on;
explain (costs off) select * from xform_test where xform_test ==> 'login:falcao5';

set enable_seqscan to off;
set enable_bitmapscan to off;
set enable_indexscan to on;
-- but this still a sequential scan
explain (costs off) select * from xform_test where xform_test ==> 'login:falcao5';
-- and this an index scan
explain (costs off) select * from xform_test where xform(xform_test) ==> 'login:falcao5';

select id from xform_test where xform(xform_test) ==> 'login:falcao5' order by id;
select zdb.score(ctid) > 0.0 as score, id from xform_test where xform(xform_test) ==> 'login:falcao5' order by id;

select id from xform_test where xform_test ==> 'login:falcao5' order by id;
select zdb.score(ctid) > 0.0 as score, id from xform_test where xform_test ==> 'login:falcao5' order by id;

DROP TABLE xform_test CASCADE;
