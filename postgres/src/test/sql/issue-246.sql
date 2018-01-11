create table issue246 (
  id serial8,
  data json
);

create index idxissue246 on issue246 using zombodb (zdb('issue246', ctid), zdb(issue246)) with (url='localhost:9200/');

insert into issue246 (data) values ('[{"id":1, "state_id": 42},{"id":2, "state_id": 42}]');
insert into issue246 (data) values ('[{"id":3, "state_id": 66},{"id":4, "state_id": 42},{"id":5, "state_id": 66}]');
insert into issue246 (data) values ('[{"id":6, "state_id": [42,66]}]');
insert into issue246 (data) values ('[{"id":7, "state_id": [42,66]},{"id":8, "state_id": 75}]');

select * from issue246 order by id;
select * from issue246 where zdb('issue246', ctid) ==> 'data.state_id = 42 and data.state_id = 66' order by id;
select * from issue246 where zdb('issue246', ctid) ==> 'data.state_id = 42 with data.state_id = 66' order by id;

select * from zdb_tally('issue246', 'data.id', true, '^.*', '', 5000, 'term');
select * from zdb_tally('issue246', 'data.id', true, '^.*', 'data.state_id=42 and data.state_id=66', 5000, 'term');
select * from zdb_tally('issue246', 'data.id', true, '^.*', 'data.state_id=42 with data.state_id=66', 5000, 'term');

select * from zdb_tally('issue246', 'data.id', false, '^.*', '', 5000, 'term');
select * from zdb_tally('issue246', 'data.id', false, '^.*', 'data.state_id=42 and data.state_id=66', 5000, 'term');
select * from zdb_tally('issue246', 'data.id', false, '^.*', 'data.state_id=42 with data.state_id=66', 5000, 'term');

select * from zdb_tally('issue246', 'id', false, '^.*', '', 5000, 'term');
select * from zdb_tally('issue246', 'id', false, '^.*', 'data.state_id=42 and data.state_id=66', 5000, 'term');
select * from zdb_tally('issue246', 'id', false, '^.*', 'data.state_id=42 with data.state_id=66', 5000, 'term');


drop table issue246 cascade;