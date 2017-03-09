CREATE TABLE public.issue_169(
   pk_data BIGINT
  ,myfield TEXT
  ,CONSTRAINT idx_data_pkey PRIMARY KEY (pk_data)
);

CREATE INDEX es_public_issue_169 ON public.issue_169 USING zombodb (zdb(issue_169), zdb_to_json(issue_169.*)) WITH (url='http://127.0.0.1:9200/', shards='3', replicas='1');

INSERT INTO public.issue_169(pk_data) VALUES (1);
INSERT INTO public.issue_169(pk_data) VALUES (2);
INSERT INTO public.issue_169(pk_data) VALUES (3);
INSERT INTO public.issue_169(pk_data) VALUES (7);
INSERT INTO public.issue_169(pk_data) VALUES (8);
INSERT INTO public.issue_169(pk_data) VALUES (9);

COPY public.issue_169(pk_data) FROM STDIN;
4
5
6
\.

SELECT pk_data FROM public.issue_169 WHERE zdb(issue_169) ==> 'pk_data:"*"' ORDER BY pk_data;

/*
 test it with batch mode off (the default) in a transaction
 We should see the results from the select
 */
TRUNCATE TABLE issue_169;
BEGIN;
SET zombodb.batch_mode TO off;
COPY public.issue_169(pk_data) FROM STDIN;
4
5
6
\.

SELECT pk_data FROM public.issue_169 WHERE zdb(issue_169) ==> 'pk_data:"*"' ORDER BY pk_data;
COMMIT;


/*
 test it with batch mode on in a transaction
 We should NOT see the results from the select until we commit
 */
TRUNCATE TABLE issue_169;
BEGIN;
SET zombodb.batch_mode TO on;
COPY public.issue_169(pk_data) FROM STDIN;
4
5
6
\.

SELECT pk_data FROM public.issue_169 WHERE zdb(issue_169) ==> 'pk_data:"*"' ORDER BY pk_data;
COMMIT;
SELECT pk_data FROM public.issue_169 WHERE zdb(issue_169) ==> 'pk_data:"*"' ORDER BY pk_data;

DROP TABLE issue_169;

