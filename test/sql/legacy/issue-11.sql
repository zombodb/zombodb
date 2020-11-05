BEGIN;

CREATE SCHEMA issue_11;
CREATE TABLE issue_11.test_table ();
SELECT count_of_table('issue_11.test_table');

ABORT;