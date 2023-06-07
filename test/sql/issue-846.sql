CREATE TABLE notnull_main (
                              pk_main SERIAL8 NOT NULL PRIMARY KEY,
                              name varchar NOT NULL
);
INSERT INTO notnull_main (name) VALUES ('Taffy');
INSERT INTO notnull_main (name) VALUES ('Tardis');
INSERT INTO notnull_main (name) VALUES ('Tackle');
INSERT INTO notnull_main (name) VALUES ('Beret');

CREATE TABLE notnull_docs (
                              pk_doc SERIAL8 NOT NULL PRIMARY KEY,
                              fk_docs_to_main int8[],
                              doctype varchar NOT NULL,
                              page_count int8
);
INSERT INTO notnull_docs (fk_docs_to_main, doctype, page_count) VALUES (ARRAY[1], 'Instructions',30);
INSERT INTO notnull_docs (fk_docs_to_main, doctype, page_count) VALUES (ARRAY[1], 'Poem',40);
INSERT INTO notnull_docs (fk_docs_to_main, doctype, page_count) VALUES (ARRAY[1,2], 'Poem',40);
INSERT INTO notnull_docs (fk_docs_to_main, doctype, page_count) VALUES (ARRAY[3], 'Instructions',50);
INSERT INTO notnull_docs (fk_docs_to_main, doctype, page_count) VALUES (ARRAY[4], 'Poem',60);

CREATE VIEW notnull_view AS
SELECT notnull_docs.pk_doc,
       notnull_docs.doctype,
       notnull_docs.page_count,
       ( SELECT array_agg(notnull_main.name) AS name
         FROM notnull_main
         WHERE notnull_main.pk_main = ANY(notnull_docs.fk_docs_to_main)
       ) AS name,
       notnull_docs.*::notnull_docs AS zdb
FROM notnull_docs;

CREATE INDEX es_idxnotnullmain
    ON notnull_main
        USING zombodb ((notnull_main.*));

CREATE INDEX es_idxnotnulldocs
    ON notnull_docs
        USING zombodb ((notnull_docs.*))
    WITH (
    max_analyze_token_count='2147483647',
    bulk_concurrency='1',
    options='fk_docs_to_main=<public.notnull_main.es_idxnotnullmain>pk_main');

SELECT * FROM notnull_view order by pk_doc;
SELECT * FROM notnull_view WHERE zdb==>'doctype=INSTRUCTIONS AND page_count <> 40' order by pk_doc;
SELECT * FROM notnull_view WHERE zdb==>'doctype=INSTRUCTIONS AND NOT page_count = 40' order by pk_doc;
SELECT upper(term) term, count
FROM zdb.tally(
        'notnull_view'::regclass,
        'name',
        'FALSE',
        '^.*',
        'doctype=INSTRUCTIONS AND page_count <> 40'::zdbquery,
        2147483647,
        'term'::termsorderby);

SELECT upper(term) term, count
FROM zdb.tally(
        'notnull_view'::regclass,
        'name',
        'FALSE',
        '^.*',
        'doctype=INSTRUCTIONS AND NOT page_count = 40'::zdbquery,
        2147483647,
        'term'::termsorderby);


drop table notnull_docs, notnull_main cascade;

