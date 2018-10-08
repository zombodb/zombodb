with sub as (select zdb.score(ctid) <> 0 as got_score, id from events where events ==> 'beer' order by id)
select * from sub;

select got_score > 0, count(*) from (
    with one as (select zdb.score(ctid) as got_score, id from events where events ==> 'beer' order by id),
         two as (select -1, id from users where users ==> 'test'),   -- should not request scores from ES
         three as (select -1, id from events where events ==> 'three')   -- should not request scores from ES
    select * from one union all select * from two union all select * from three order by id
) x group by 1 order by 1;

create or replace function test_286 () returns bigint language sql as $$
    with sub as (select zdb.score(ctid) <> 0 as got_score, id from events where events ==> 'beer' order by id)
    select count(*) from sub where got_score;
$$;
select test_286();

select id from events where events ==> 'beer' and zdb.score(ctid) > 0.0 order by id;

select * from (select id from events where events ==> 'beer' order by zdb.score(ctid)) x order by id;
select * from (select id from events where events ==> 'beer' order by zdb.score(ctid) asc) x order by id;
select * from (select id from events where events ==> 'beer' order by zdb.score(ctid) desc) x order by id;
