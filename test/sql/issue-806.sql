CREATE TABLE hyphen_test (
                             id SERIAL8 NOT NULL PRIMARY KEY,
                             full_text zdb.fulltext NOT NULL
);
CREATE INDEX es_idxhyphentest_var
    ON hyphen_test USING zombodb ((hyphen_test.*));

insert into hyphen_test(full_text) values ('please remove me from your e-mail list');
insert into hyphen_test(full_text) values ('please send me your e-mails');

SELECT * FROM hyphen_test ORDER BY 1;
SELECT * FROM hyphen_test WHERE hyphen_test ==> 'full_text : "e-mail"' ORDER BY 1;
SELECT * FROM hyphen_test WHERE hyphen_test ==> 'full_text : "e*"' ORDER BY 1;
SELECT * FROM hyphen_test WHERE hyphen_test ==> 'full_text : "e-mail*"' ORDER BY 1;

drop table hyphen_test;