create table issue291 (id serial8 not null primary key, title text);
create index idxissue292 on issue291 using zombodb (zdb('issue291', ctid), zdb(issue291)) with (url='localhost:9200/');
insert into issue291 (title) values ('one');
insert into issue291 (title) values ('two');
insert into issue291 (title) values ('three');
insert into issue291 (title) values ('four');
insert into issue291 (title) values ('five');
insert into issue291 (title) values ('six');
insert into issue291 (title) values ('seven');
insert into issue291 (title) values ('eight');
insert into issue291 (title) values ('nine');
insert into issue291 (title) values ('ten');

select * from issue291 where zdb('issue291', ctid) ==> '#limit(title desc, 0, 5)' order by id limit 5;

update issue291 set id = id;
select * from issue291 where zdb('issue291', ctid) ==> '#limit(title desc, 0, 5)' order by id limit 5;

drop table issue291;