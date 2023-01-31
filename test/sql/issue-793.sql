CREATE TABLE issue793
(
    pkey          serial8,
    date_combined date
);
CREATE INDEX idxissue793 ON issue793 USING zombodb ((issue793.*));

INSERT INTO issue793 (date_combined)
VALUES ('2020-05-10'),
       ('2021-08-01'),
       ('2022-03-13'),
       ('1999-12-31'),
       ('1976-07-04');


-- Correct output, stem is '^.*'
SELECT term::date as term, count, term AS exact_term
FROM zdb.tally('issue793'::regclass, 'date_combined', 'FALSE', '^.*', ''::zdbquery, 5000, 'term'::termsorderby);


-- Stem is '^1.*' - expecting two rows
SELECT term::date as term, count, term AS exact_term
FROM zdb.tally('issue793'::regclass, 'date_combined', 'FALSE', '^1.*', ''::zdbquery, 5000, 'term'::termsorderby);

-- Attempt to use `.date` subfield just in case
SELECT term::date as term, count, term AS exact_term
FROM zdb.tally('issue793'::regclass, 'date_combined.date', 'FALSE', '^1.*', ''::zdbquery, 5000, 'term'::termsorderby);

DROP TABLE issue793;