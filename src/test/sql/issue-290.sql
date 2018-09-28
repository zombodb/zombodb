create table issue290 (
    id serial8 not null primary key,
    data text
);
create index idxissue290 on issue290 using zombodb ((issue290.*));
insert into issue290 (data) values ('😂 — test');

select zdb.highlight(ctid, 'data'), id from issue290 where issue290 ==> 'test';

drop table issue290;