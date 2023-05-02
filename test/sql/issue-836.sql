CREATE TABLE issue836 (
    id serial8 not null primary key,
    b bool
);
insert into issue836 (id, b) values (default, false);
insert into issue836 (id, b) values (default, true);
create index idxissue836 on issue836 using zombodb ((issue836.*));

select * from zdb.highlight_document('issue836', (select row_to_json(issue836) from issue836 where b = true), 'b:true or b:false or b:TRUE or b:FALSE or b:NOT_a_Boolean', false);
select * from zdb.highlight_document('issue836', (select row_to_json(issue836) from issue836 where b = false), 'b:true or b:false or b:TRUE or b:FALSE or b:NOT_a_Boolean', false);

drop table issue836 cascade;