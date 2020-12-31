create table issue624 (title text);
insert into issue624 select convert_from(decode('QXRlbGllcnMgZOKAmcOpY2hhbmdlIGV0IGRlIHByYXRpcXVlIHBvdXIgC2xlcyBtYW5hZ2VycyBvcMOpcmF0aW9ubmVscw==', 'base64'), 'UTF8');
select * from issue624;
create index idxissue624 on issue624 using zombodb ((issue624.*));
select * from zdb.terms('issue624', 'title', '') order by term;
drop table issue624;