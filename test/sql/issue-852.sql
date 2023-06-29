create table issue852 (
    body text[]
);
create index idxissue852 on issue852 using zombodb ((issue852.*));
insert into issue852(body) values (NULL);
insert into issue852(body) values (ARRAY[NULL, NULL, NULL]);
insert into issue852(body) values (ARRAY['a']);
insert into issue852(body) values (ARRAY['a', 'b']);
insert into issue852(body) values (ARRAY['a', 'b', 'c']);


SELECT count(*) FROM issue852 WHERE issue852 ==> 'body:[]';
SELECT count(*) FROM issue852 WHERE issue852 ==> 'body<>[]';
select * from zdb.dump_query('idxissue852', 'body:[]');
select * from zdb.debug_query('idxissue852', 'body:[]');
select * from zdb.dump_query('idxissue852', 'body<>[]');
select * from zdb.debug_query('idxissue852', 'body<>[]');

SELECT count(*) FROM issue852 WHERE issue852 ==> 'body:[[]]';
SELECT count(*) FROM issue852 WHERE issue852 ==> 'body<>[[]]';
select * from zdb.dump_query('idxissue852', 'body:[[]]');
select * from zdb.debug_query('idxissue852', 'body:[[]]');
select * from zdb.dump_query('idxissue852', 'body<>[[]]');
select * from zdb.debug_query('idxissue852', 'body<>[[]]');
drop table issue852;