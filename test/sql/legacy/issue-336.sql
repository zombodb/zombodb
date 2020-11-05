create table scaled_float_test(
  id bigserial primary key,
  scale_3 numeric(10,3)
);

select * from zdb.define_field_mapping('scaled_float_test','scale_3','{
  "type" : "scaled_float",
  "scaling_factor": 1000
}');

CREATE INDEX idx_zdb_scaled_float_test
  ON scaled_float_test
  USING zombodb( (scaled_float_test.*));

insert into scaled_float_test (scale_3) values(0.480);
insert into scaled_float_test (scale_3) values(0.636);
insert into scaled_float_test (scale_3) values(0.637);

select id, scale_3,CTID from scaled_float_test where scaled_float_test ==> 'scale_3 < 0.637' order by id;

drop table scaled_float_test;
