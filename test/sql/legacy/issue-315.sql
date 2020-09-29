CREATE TABLE a (
  pk_a        BIGINT NOT NULL,
  fk_a_to_map BIGINT
);

CREATE TABLE mapping (
  pk_map bigint NOT NULL,
  fk_map_to_b bigint
);

CREATE TABLE b (
  pk_b_id    BIGINT NOT NULL,
  b_group_id TEXT
);

create index idxb on b using zombodb (zdb('b', ctid), zdb(b)) with (url='localhost:9200/');
create index idxmapping on mapping using zombodb (zdb('mapping', ctid), zdb(mapping)) with (url='localhost:9200/');
create index idxa on a using zombodb (zdb('a', ctid), zdb(a)) with (url='localhost:9200/',
      options='fk_a_to_map=<mapping.idxmapping>pk_map,
               fk_map_to_b=<b.idxb>pk_b_id',
      always_resolve_joins=true);


insert into b(pk_b_id, b_group_id) values(100, 42);
insert into b(pk_b_id, b_group_id) values(200, 42);
insert into b(pk_b_id, b_group_id) values(300, 0);

insert into mapping(pk_map, fk_map_to_b) values(1, 100);
insert into mapping(pk_map, fk_map_to_b) values(2, 300);
insert into mapping(pk_map, fk_map_to_b) values(3, 200);

insert into a (pk_a, fk_a_to_map) values(10, 1);
insert into a (pk_a, fk_a_to_map) values(20, 2);
insert into a (pk_a, fk_a_to_map) values(30, 3);

select * from a where zdb('a', ctid) ==> '#expand<b_group_id=<this.index>b_group_id>(pk_b_id = 100)' order by pk_a;

set zombodb.ignore_visibility to on; /* to avoid transient xact data in query output */
select (zdb_dump_query('a', '#expand<b_group_id=<this.index>b_group_id>(pk_b_id = 100)')::json)->'cross_join'->'query';

drop table a cascade;
drop table b cascade;
drop table mapping cascade;