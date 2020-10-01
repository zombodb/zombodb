create table issue338
(
  id  bigint,
  md5 varchar
);
insert into issue338 select v, md5(v::text) from generate_series(1, 100000) v;
create index idxissue338 on issue338 using zombodb ( (issue338.*) );

select count(*) from issue338
  where issue338  ==>
        '#expand<id=<this.index>id>(#expand<id=<this.index>id>(#expand<md5=<this.index>md5>(md5:*)))';

drop table issue338;