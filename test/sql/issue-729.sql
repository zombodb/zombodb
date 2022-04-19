CREATE TABLE tst_pagination
(
    id    SERIAL
        CONSTRAINT tst_pagination_pk PRIMARY KEY,
    title VARCHAR
);

CREATE INDEX idx_tst_pagination
    ON tst_pagination
    USING zombodb ((tst_pagination.*));

INSERT INTO tst_pagination (title)
VALUES ('Fruits: orange'),
       ('Fruits: mango'),
       ('Fruits: apple'),
       ('Fruits: pear'),
       ('Fruits: kiwi');

--without offset - its ok
SELECT zdb.score(ctid) > 0 as score,
       id,
       title
FROM tst_pagination
WHERE tst_pagination ==> dsl.limit(2, dsl.offset(0, dsl.sort('_score', 'desc', 'fruits'::text)))
ORDER BY id;

-- with offset with score = 0 :(
SELECT zdb.score(ctid) > 0 as score,
       id,
       title
FROM tst_pagination
WHERE tst_pagination ==> dsl.limit(2, dsl.offset(2, dsl.sort('_score', 'desc', 'fruits'::text)))
ORDER BY id;

DROP TABLE tst_pagination CASCADE;