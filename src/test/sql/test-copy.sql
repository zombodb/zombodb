CREATE TABLE copy_test(
   pkey BIGINT
  ,myfield TEXT
  ,CONSTRAINT idx_data_pkey PRIMARY KEY (pkey)
);

CREATE INDEX idxcopy_test ON copy_test USING zombodb ((copy_test));

INSERT INTO copy_test(pkey) VALUES (1);
INSERT INTO copy_test(pkey) VALUES (2);
INSERT INTO copy_test(pkey) VALUES (3);
INSERT INTO copy_test(pkey) VALUES (7);
INSERT INTO copy_test(pkey) VALUES (8);
INSERT INTO copy_test(pkey) VALUES (9);

COPY copy_test(pkey) FROM STDIN;
4
5
6
\.

SELECT pkey FROM copy_test WHERE copy_test ==> range(field=>'pkey', gte=>0) ORDER BY pkey;

/*
 test it with batch mode off (the default) in a transaction
 We should see the results from the select
 */
TRUNCATE TABLE copy_test;
BEGIN;
SET zdb.batch_mode TO off;
COPY copy_test(pkey) FROM STDIN;
4
5
6
\.

SELECT pkey FROM copy_test WHERE copy_test ==> range(field=>'pkey', gte=>0) ORDER BY pkey;
COMMIT;


/*
 test it with batch mode on in a transaction
 We should NOT see the results from the select until we commit
 */
TRUNCATE TABLE copy_test;
BEGIN;
SET zdb.batch_mode TO on;
COPY copy_test(pkey) FROM STDIN;
4
5
6
\.

SELECT pkey FROM copy_test WHERE copy_test ==> range(field=>'pkey', gte=>0) ORDER BY pkey;
COMMIT;
SELECT pkey FROM copy_test WHERE copy_test ==> range(field=>'pkey', gte=>0) ORDER BY pkey;

DROP TABLE copy_test;

