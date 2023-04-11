CREATE TABLE issue828
(
    id        SERIAL8                    NOT NULL PRIMARY KEY,
    full_text zdb.fulltext_with_shingles NOT NULL
);
CREATE INDEX es_idxhyphenshinglestest_var
    ON issue828 USING zombodb ((issue828.*));

insert into issue828(full_text)
values ('please remove me from your e-mail list');
insert into issue828(full_text)
values ('please send me your e-mails');

SELECT *
FROM issue828
WHERE issue828 ==> '(full_text : "e-mail*" )';

DROP TABLE issue828;