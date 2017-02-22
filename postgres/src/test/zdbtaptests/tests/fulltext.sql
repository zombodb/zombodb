-- USAGE: $ pg_prove -d zdbtaptests -U postgres src/test/zdbtaptests/tests/fulltext.sql

-- Start transaction and plan the tests.
BEGIN;
SELECT plan(66);

-- Run the tests.
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"chuck"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"chuck"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"chuck norris"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"chuck norris"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"norri?"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltest: norri?');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"norri?"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: norri?');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"norri*"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: norri*');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"norri*"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: norri*');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/0 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/0 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/1 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/1 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/2 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/2 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/3 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/3 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/4 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/4 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/5 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/5 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"chuck" AND data_full_text:"kill"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck AND fulltext: kill');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"chuck" OR data_full_text:"kill"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck OR fulltext: kill');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"chuck" NOT data_full_text:"kill"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck NOT fulltext: kill');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"chuck" AND data_full_text_shingles:"kill"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck AND shingles: kill');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"chuck" OR data_full_text_shingles:"kill"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck OR shingles: kill');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 3, 4, 5, 6, 8, 9, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"chuck" NOT data_full_text_shingles:"kill"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck NOT shingles: kill');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text:"chuck" OR data_full_text:"norris") w/0 data_full_text:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck OR fulltext: norris w/0 fulltext: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text:"chuck" OR data_full_text:"norris") w/1 data_full_text:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck OR fulltext: norris w/1 fulltext: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text:"chuck" OR data_full_text:"norris") w/2 data_full_text:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck OR fulltext: norris w/2 fulltext: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text:"chuck" OR data_full_text:"norris") w/3 data_full_text:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck OR fulltext: norris w/3 fulltext: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris") w/0 data_full_text_shingles:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck OR shingles: norris w/0 shingles: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris") w/1 data_full_text_shingles:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck OR shingles: norris w/1 shingles: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris") w/2 data_full_text_shingles:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck OR shingles: norris w/2 shingles: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris") w/3 data_full_text_shingles:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck OR shingles: norris w/3 shingles: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text:"chuck" OR data_full_text:"norris") wo/3 data_full_text:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck OR fulltext: norris wo/3 fulltext: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text:"chuck" OR data_full_text:"norris") wo/2 data_full_text:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck OR fulltext: norris wo/2 fulltext: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text:"chuck" OR data_full_text:"norris") wo/1 data_full_text:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: chuck OR fulltext: norris wo/1 fulltext: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris") wo/3 data_full_text_shingles:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck OR shingles: norris wo/3 shingles: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris") wo/2 data_full_text_shingles:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck OR shingles: norris wo/2 shingles: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT * FROM (SELECT 1 WHERE 1=2)x; --this returns an empty result set
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'((data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris") wo/1 data_full_text_shingles:"in")';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: chuck OR shingles: norris wo/1 shingles: in');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'(data_full_text:"in" wo/3 (data_full_text:"chuck" OR data_full_text:"norris"))';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: in wo/3 fulltext: chuck OR fulltext: norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'(data_full_text:"in" wo/2 (data_full_text:"chuck" OR data_full_text:"norris"))';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: in wo/2 fulltext: chuck OR fulltext: norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'(data_full_text:"in" wo/1 (data_full_text:"chuck" OR data_full_text:"norris"))';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: in wo/1 fulltext: chuck OR fulltext: norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'(data_full_text_shingles:"in" wo/3 (data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris"))';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: in wo/3 shingles: chuck OR shingles: norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'(data_full_text_shingles:"in" wo/2 (data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris"))';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: in wo/2 shingles: chuck OR shingles: norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6, 10]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'(data_full_text_shingles:"in" wo/1 (data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris"))';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: in wo/1 shingles: chuck OR shingles: norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 7, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'NOT (data_full_text:"in" wo/1 (data_full_text:"chuck" OR data_full_text:"norris"))';
SELECT set_eq('expected_result', 'zdb_result', 'NOT fulltext: in wo/1 fulltext: chuck OR fulltext: norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[1, 2, 3, 4, 5, 7, 8, 9]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'NOT (data_full_text_shingles:"in" wo/1 (data_full_text_shingles:"chuck" OR data_full_text_shingles:"norris"))';
SELECT set_eq('expected_result', 'zdb_result', 'NOT shingles: in wo/1 shingles: chuck OR shingles: norris');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"n??????"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: n??????');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[2]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"n??????"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: n??????');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"p??????"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: p??????');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"p??????"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: p??????');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[8]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text:"n???"';
SELECT set_eq('expected_result', 'zdb_result', 'fulltext: p??????');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[8]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view where zdb==>'data_full_text_shingles:"n???"';
SELECT set_eq('expected_result', 'zdb_result', 'shingles: p??????');
--**********************************************************************************************************************

--This section is here specifically to stress the SIREn plugin cache mechanism
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/3 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/3 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/3 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/3 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/3 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/3 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/3 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/3 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/3 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/3 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/3 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/3 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/3 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/3 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
DEALLOCATE ALL;
PREPARE expected_result AS SELECT unnest(ARRAY[3, 6]::BIGINT[]);
PREPARE zdb_result AS SELECT pk_data FROM unit_tests.consolidated_record_view WHERE zdb ==> 'data_phrase_1:("jack" w/6 "master")';
SELECT set_eq('expected_result', 'zdb_result', 'jack" w/6 "master');
--**********************************************************************************************************************
--This section was here specifically to stress the SIREn plugin cache mechanism


-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;
