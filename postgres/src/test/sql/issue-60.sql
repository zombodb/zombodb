create table public.mam_test_json(pk_id serial8 not null primary key,my_json json);
CREATE INDEX es_mam_test_json ON public.mam_test_json USING zombodb (zdb('mam_test_json', ctid), zdb(mam_test_json.*))
WITH (url='http://localhost:9200/', replicas=1, shards=5);

insert into public.mam_test_json(my_json) values('[{"sub_id":"1","sub_state":"NC","sub_status":"A"},{"sub_id":"2","sub_state":"SC","sub_status":"I"}]');
insert into public.mam_test_json(my_json) values('[{"sub_id":"1","sub_state":"NC","sub_status":"A"}]');

select * from mam_test_json where zdb('mam_test_json', ctid) ==> '(my_json.sub_state:"SC" with my_json.sub_status:"I")
            AND (my_json.sub_state:"NC" with my_json.sub_status:"A")';

select * from mam_test_json where zdb('mam_test_json', ctid) ==> '(my_json.sub_state:"NC" AND my_json.sub_state:SC)';

select * from mam_test_json where zdb('mam_test_json', ctid) ==> '(my_json.sub_state:"NC" OR my_json.sub_state:SC)' order by pk_id;

select * from mam_test_json where zdb('mam_test_json', ctid) ==> '(my_json.sub_state:"NC" WITH my_json.sub_state:SC)';

drop table mam_test_json;
