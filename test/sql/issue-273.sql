CREATE TABLE issue273 (
    id bigint not null primary key,
    data text
);
insert into issue273 (id, data) values (1, 'this is a test');
create index idxissue273 on issue273 using zombodb ((issue273.*));
begin;
select txid_current() is not null;  -- burn current xid
savepoint foo;
update issue273 set id = id where id = 1;  -- gets next xid value
commit; -- results in ERROR because "next xid value" doesn't exist in events' list of aborted xids.


begin;
insert into issue273(id) values (2);
select * from issue273 order by id;
select * from zdb.terms('idxissue273', 'id', dsl.match_all(), 1000, 'term');
savepoint three;
insert into issue273(id) values (3);
select * from issue273 order by id;
select * from zdb.terms('idxissue273', 'id', dsl.match_all(), 1000, 'term');
rollback to three;
insert into issue273(id) values (4);
select * from issue273 order by id;
select * from zdb.terms('idxissue273', 'id', dsl.match_all(), 1000, 'term');
commit;

select * from issue273 order by id;
select * from zdb.terms('idxissue273', 'id', dsl.match_all(), 1000, 'term');

SELECT jsonb_array_length((zdb.request('idxissue273', '_doc/zdb_aborted_xids?pretty')::jsonb)->'_source'->'zdb_aborted_xids');
VACUUM issue273;
SELECT jsonb_array_length((zdb.request('idxissue273', '_doc/zdb_aborted_xids?pretty')::jsonb)->'_source'->'zdb_aborted_xids');


DROP TABLE issue273;
