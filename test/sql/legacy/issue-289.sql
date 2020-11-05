create table issue289 (
  id serial8 not null primary key,
  json_field json
);

insert into issue289 (id) values (default);

create index idxissue289 on issue289 using zombodb ( (issue289.*) );

select * from issue289 where issue289 ==> 'json_field = null';

-- this is the form that's bugged
select * from issue289 where issue289 ==> 'json_field:null';

drop table issue289;