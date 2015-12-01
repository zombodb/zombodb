CREATE SCHEMA issue_66;
CREATE TABLE issue_66.test(id serial8 not null primary key);
CREATE INDEX idxissue_66_test ON issue_66.test USING zombodb (zdb('issue_66.test', ctid), zdb(test)) WITH (url='http://localhost:9200/');

CREATE SCHEMA issue_66_other;
CREATE TABLE issue_66_other.test(id serial8 not null primary key);
CREATE INDEX idxissue_66_test ON issue_66_other.test USING zombodb (zdb('issue_66_other.test', ctid), zdb(test)) WITH (url='http://localhost:9200/');


SELECT zdb_determine_index('issue_66.test')::regclass = 'issue_66.idxissue_66_test'::regclass;
SELECT zdb_determine_index('issue_66_other.test')::regclass = 'issue_66_other.idxissue_66_test'::regclass;


DROP SCHEMA issue_66 CASCADE;
DROP SCHEMA issue_66_other CASCADE;