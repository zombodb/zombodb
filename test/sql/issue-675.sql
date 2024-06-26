create table issue675 (uri varchar not null, text zdb.fulltext, primary key (uri));
insert into issue675 values ('uri','foo');
create index if not exists idxissue675 on issue675 using zombodb((issue675.*));
select count(*) from issue675 where issue675 ==> 'text:"foo bar"'; -- 0 rows found (correct)
select count(*) from issue675 where issue675 ==> 'text:"foo-bar"'; -- 1 row found (incorrect)

drop table issue675 cascade;