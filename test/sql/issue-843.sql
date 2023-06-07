CREATE TABLE issue843 (
                                         id SERIAL8 NOT NULL PRIMARY KEY,
                                         name varchar NOT NULL
);
CREATE INDEX es_idxwildcardedkeywordtest
    ON issue843 USING zombodb ((issue843.*));

INSERT INTO issue843 (name) values ('Pancake');
INSERT INTO issue843 (name) values ('Waffle');
INSERT INTO issue843 (name) values ('French Toast');
INSERT INTO issue843 (name) values ('Cinnamon Toast');

SELECT * FROM issue843 order by id;
SELECT * FROM issue843 WHERE issue843 ==> 'name = "*Toast"' order by id;
SELECT * FROM issue843 WHERE issue843 ==> 'name = "*Toa*"' order by id;
SELECT * FROM issue843 WHERE issue843 ==> 'name = "*ench Toa*"' order by id;

DROP TABLE issue843;