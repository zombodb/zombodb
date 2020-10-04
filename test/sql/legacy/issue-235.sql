--
-- requires that zombodb.default_elasticsearch_url be set in postgresql.conf
--
CREATE TABLE issue_235 AS SELECT oid::bigint AS id, relname::text AS relname FROM pg_class LIMIT 10; -- just need some data from somewhere
CREATE INDEX idxissue_235 ON issue_235 USING zombodb ( (issue_235.*) ) WITH (url='default');
SELECT count(*) FROM issue_235 WHERE issue_235 ==> '';
DROP TABLE issue_235 CASCADE;
