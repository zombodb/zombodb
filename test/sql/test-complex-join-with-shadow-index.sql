create table master
(
    p_id serial8 not null primary key
);
create table secondary
(
    s_id serial8 not null primary key
);

create index idxsecondary on secondary using zombodb ((secondary.*));

create function shadow_idx_fn(anyelement) returns anyelement immutable strict
    language c as
'$libdir/zombodb.so',
'shadow_wrapper';
create index idxprimary on master using zombodb ((master.*));
create index idxprimary_shadow on master using zombodb ((shadow_idx_fn(master.*))) with (shadow='true',
    options= ' p_id = <public.secondary.idxsecondary>s_id');

insert into secondary(s_id)
values (1);
insert into master(p_id)
values (1);

create view together as
select master.p_id, secondary.s_id, secondary.ctid, shadow_idx_fn(secondary) as zdb
from master
         join secondary on master.p_id = secondary.s_id;

SELECT * FROM together ORDER BY p_id;
SELECT * FROM together WHERE zdb ==> 'p_id:1';
SELECT * FROM together WHERE zdb ==> 's_id:1';

drop view together;
drop table master cascade;
drop table secondary cascade;
drop function shadow_idx_fn;
