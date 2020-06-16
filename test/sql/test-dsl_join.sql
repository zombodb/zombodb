SELECT id FROM events WHERE events ==> dsl.sort('id', 'asc', dsl.limit(10, dsl.join('user_id', 'idxusers', 'id', 'id:255802'))) order by id;
SELECT id FROM events WHERE events ==> dsl.sort('id', 'asc', dsl.limit(10, dsl.join('user_id', 'idxusers', 'id', 'id:255802', 10))) order by id;
SELECT id FROM events WHERE events ==> dsl.sort('id', 'asc', dsl.limit(10, dsl.join('user_id', 'idxusers', 'id', 'id:"-1"'))) order by id;
SELECT id FROM events WHERE events ==> dsl.sort('id', 'asc', dsl.limit(10, dsl.join('user_id', 'idxusers', 'id', 'id:"-1"', 10))) order by id;

SELECT jsonb_pretty(dsl.join('user_id', 'idxusers', 'id', 'id:255802'));
SELECT jsonb_pretty(dsl.join('user_id', 'idxusers', 'id', 'id:"-1"'));