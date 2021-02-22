create table test_zombodb ( id integer not null primary key);
insert into test_zombodb (id) values (1);
create type test_zombodb_es as (id integer, text text);

create function index(test_zombodb test_zombodb) returns test_zombodb_es as $$
select row(test_zombodb.id, 'some text')::test_zombodb_es
$$ language sql immutable strict;

create index idx_es_test_zombodb
    on test_zombodb using zombodb(index(test_zombodb));

explain select zdb.score(ctid) > 0.0, index(test_zombodb) from test_zombodb where index(test_zombodb) ==> 'text';
select zdb.score(ctid) > 0.0, index(test_zombodb) from test_zombodb where index(test_zombodb) ==> 'text';

set enable_indexscan to off;
explain select zdb.score(ctid) > 0.0, index(test_zombodb) from test_zombodb where index(test_zombodb) ==> 'text';
select zdb.score(ctid) > 0.0, index(test_zombodb) from test_zombodb where index(test_zombodb) ==> 'text';

set enable_bitmapscan to off;
explain select zdb.score(ctid) > 0.0, index(test_zombodb) from test_zombodb where index(test_zombodb) ==> 'text';
select zdb.score(ctid) > 0.0, index(test_zombodb) from test_zombodb where index(test_zombodb) ==> 'text';


drop table test_zombodb cascade;
drop type test_zombodb_es cascade;
