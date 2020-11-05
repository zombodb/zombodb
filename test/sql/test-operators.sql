-- these do things
SELECT count(*) FROM events WHERE events ==> 'beer';
SELECT count(*) FROM events WHERE events ==| ARRAY['beer', 'wine', 'cheese'];
SELECT count(*) FROM events WHERE events ==& ARRAY['foo', 'bar'];
SELECT count(*) FROM events WHERE events ==! ARRAY['beer', 'wine', 'cheese'];

-- these raise errors
select 'a'::text ==| array['foo', 'bar']::zdbquery[];
select 'a'::text ==& array['foo', 'bar']::zdbquery[];
select 'a'::text ==! array['foo', 'bar']::zdbquery[];
select 'a'::text ==> 'foo'::zdbquery;