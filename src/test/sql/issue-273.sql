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
DROP TABLE issue273;
