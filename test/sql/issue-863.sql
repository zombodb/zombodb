CREATE TABLE issue863
(
    id        SERIAL8                    NOT NULL PRIMARY KEY,
    full_text zdb.fulltext_with_shingles NOT NULL
);
CREATE INDEX idxissue863
    ON issue863 USING zombodb ((issue863.*));

INSERT INTO issue863(full_text)
values ('It is rumored that they were one of Cleopatra''s prized beauty secrets. Pickles have been around for thousands of years, dating as far back as 2030 BC when cucumbers from their native India were pickled in the Tigris Valley.');

SELECT *
FROM zdb.highlight_document('issue863',
                            '{"full_text": "It is rumored that they were one of Cleopatra''s prized beauty secrets. Pickles have been around for thousands of years, dating as far back as 2030 BC when cucumbers from their native India were pickled in the Tigris Valley."}'::json,
                            '( full_text:("thousands of years") )'::TEXT);

DROP TABLE issue863;