CREATE TABLE tas_people
(
  pk_ppl bigserial NOT NULL,
  ppl_fname text,
  ppl_lname text,
  CONSTRAINT tas_people_pkey PRIMARY KEY (pk_ppl)
);


CREATE TABLE tas_people_text
(
  pk_ppl_text bigserial NOT NULL,
  fk_ppl bigint,
  txt_full_text fulltext_with_shingles,
  CONSTRAINT tas_people_text_pkey PRIMARY KEY (pk_ppl_text)
);

insert into tas_people(ppl_fname, ppl_lname) values ('john', 'doe'), ('jane', 'snow'), ('eric', 'bridge'), ('mary', 'poppins'), ('larry', 'hopkins'), ('sammy', 'watkins');
insert into tas_people_text(fk_ppl, txt_full_text) values (1, 'some text'), (1, 'beer wine cheese'), (2, 'beer'), (3, 'flowers'), (4, 'minivan'), (5, 'some text'), (1, 'cola'), (1, 'water'), (null, 'beer'), (null, 'beer');


--drop index public.es_tas_people_text;
CREATE INDEX es_tas_people_text ON public.tas_people_text USING zombodb (zdb(tas_people_text), zdb_to_json(tas_people_text.*)) WITH (url='http://127.0.0.1:9200/', shards=4, replicas=1);
--drop index public.es_tas_people;
CREATE INDEX es_tas_people ON public.tas_people USING zombodb (zdb(tas_people), zdb_to_json(tas_people.*)) WITH (url='http://127.0.0.1:9200/', options='pk_ppl = <tas_people_text.es_tas_people_text>fk_ppl', shards=4, replicas=1, always_resolve_joins=true);

--DROP VIEW public.tas_people_view;
CREATE OR REPLACE VIEW public.tas_people_view AS
  select pk_ppl
    ,ppl_fname
    ,ppl_lname
    ,txt_full_text
    ,zdb(tas_people) as zdb
  from tas_people
    left join tas_people_text on pk_ppl = fk_ppl;


select * from tas_people;
select * from tas_people_text;
select * from tas_people_view;

SELECT * from zdb_estimate_count('public.tas_people_text', 'txt_full_text:beer*');
SELECT * from zdb_estimate_count('public.tas_people_view', 'txt_full_text:beer*');

SELECT zdb_get_index_mapping('tas_people')->'mappings'->'data'->'_meta'->>'always_resolve_joins' = 'true';
ALTER INDEX es_tas_people SET (always_resolve_joins=false);
SELECT zdb_update_mapping('tas_people');
SELECT zdb_get_index_mapping('tas_people')->'mappings'->'data'->'_meta'->>'always_resolve_joins' = 'false';

DROP TABLE tas_people CASCADE;
DROP TABLE tas_people_text CASCADE;
