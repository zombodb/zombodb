create table issue338
(
  id  bigint,
  md5 text
);
insert into issue338 select v, md5(v::text) from generate_series(1, 100000) v;
create index idxissue338 on issue338 using zombodb (zdb('issue338', ctid), zdb(issue338)) with (url='zero:8080/', always_join_with_docvalues= true);

select count(*) from issue338
  where zdb('issue338', ctid) ==>
        '#expand<id=<this.index>id>(#expand<id=<this.index>id>(#expand<md5=<this.index>md5>(md5:*)))';

drop table issue338;