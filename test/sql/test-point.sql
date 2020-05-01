CREATE TABLE points (
  id serial8 not null primary key,
  point point
);

create index idxpoints on points using zombodb ((points.*));
insert into points(point) values ('0,0');
insert into points(point) values ('1,1');
insert into points(point) values ('2,2');

select * from points where points ==> dsl.geo_polygon('point', '0,0', '1,1', '2,2');
select * from points where points ==> dsl.geo_bounding_box('point', '0,0,2,2');

drop table points;