create table scaled_float_test(
  id bigserial primary key,
  scale_3 numeric(10,3)
);

select * from zdb_define_mapping('scaled_float_test','scale_3','{
  "type" : "scaled_float",
  "scaling_factor": 1000,
  "include_in_all" : false
}');

CREATE INDEX idx_zdb_scaled_float_test
  ON scaled_float_test
  USING zombodb(zdb('scaled_float_test', scaled_float_test.ctid), zdb(scaled_float_test))
WITH (url='http://localhost:9200/',store=true);

insert into scaled_float_test (scale_3) values(0.480);
insert into scaled_float_test (scale_3) values(0.636);
insert into scaled_float_test (scale_3) values(0.637);

select id, scale_3,CTID from scaled_float_test where zdb('scaled_float_test',CTID) ==> 'scale_3 < 0.637' order by id;

drop table scaled_float_test;
