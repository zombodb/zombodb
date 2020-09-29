create table issue252 (
  id serial8,
  data bytea
);

insert into issue252 (data) values ('123456');
create index idxissue252 on issue252 using zombodb (zdb('issue252', ctid), zdb(issue252)) with (url='localhost:9200/');

select zdb_get_index_mapping('issue252')->'mappings'->'data'->'properties'->'data'->>'type' as es_type;
drop table issue252 cascade;