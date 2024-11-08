create table wrongatt (id bigint not null primary key, other_id bigint);
insert into wrongatt(id) values (1);
create index idxwrongatt on wrongatt using zombodb ((wrongatt.*));
create view wrongatt_view as select *, (select array_agg(oid) from pg_class) oids, wrongatt as zdb from wrongatt;

create or replace function wrongatt_trigger() returns trigger language plpgsql as $$
BEGIN
    UPDATE wrongatt SET other_id = 99 WHERE id = OLD.id;
    RETURN NEW;
END;
$$;
create trigger wrongatt_viewtgr instead of update on wrongatt_view for each row execute function wrongatt_trigger();


select * from wrongatt where wrongatt ==> 'id:1';
update wrongatt_view set other_id = id where id in (select id from wrongatt_view where wrongatt_view.zdb ==> 'id:1');
select * from wrongatt where wrongatt ==> 'id:1';

drop table wrongatt cascade;