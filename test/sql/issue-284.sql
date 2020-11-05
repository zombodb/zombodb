create type issue284_index_type as (id bigint, event_type varchar);
create table issue284 as select * from events order by id limit 10;
create index idxissue284 on issue284 using zombodb ((ROW(id, event_type)::issue284_index_type));
select id from issue284 where issue284 ==> dsl.match_all() order by id;

drop table issue284;
drop type issue284_index_type;