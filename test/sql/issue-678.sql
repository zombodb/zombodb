create table issue678
(
    id          integer                not null generated by default as identity,
    name        character varying(250) not null,
    description text,
    primary key (id),
    constraint unique_name unique (name)
);
create index idxissue678 on issue678 using zombodb ((issue678.*)) with (url = 'http://localhost:9200/');

insert into issue678 (name, description)

select 'test' || numb::text, 'test value ' || numb::text
from generate_series(1, 100) gs (numb);

explain (costs off) select * from issue678 as t where t ==> 'test' order by t.name, t.id limit 1;
select * from issue678 as t where t ==> 'test' order by t.name, t.id limit 1;

drop table issue678;