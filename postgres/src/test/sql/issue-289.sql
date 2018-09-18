create table issue289 (
  id serial8 not null primary key,
  json_field json
);

insert into issue289 (id) values (default);

create index idxissue289 on issue289 using zombodb (zdb('issue289', ctid), zdb(issue289)) with (url='localhost:9200/');

select * from issue289 where zdb('issue289', ctid) ==> 'json_field = null';

-- this is the form that's bugged
select * from issue289 where zdb('issue289', ctid) ==> 'json_field:null';

drop table issue289;