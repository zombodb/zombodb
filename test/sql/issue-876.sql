CREATE TABLE issue876 (
                           id SERIAL8 NOT NULL PRIMARY KEY,
                           full_text zdb.fulltext NOT NULL
);
CREATE INDEX idxissue876
    ON issue876 USING zombodb ((issue876.*));

insert into issue876(full_text)values ('The quick brown fox jumped over the lazy dog''s back');

SELECT * FROM issue876 WHERE issue876 ==> 'full_text:(quick w/5 fox w/6 over w/15 lazy)';
SELECT * FROM issue876 WHERE issue876 ==> 'full_text:(quick w/5 fox w/6 over)';
SELECT * FROM issue876 WHERE issue876 ==> 'full_text:(quick w/5 fox w/6 over w/15 lazy w/6 back)';

DROP TABLE issue876;
