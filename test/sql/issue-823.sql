CREATE TABLE issue823
(
    id         SERIAL8 NOT NULL PRIMARY KEY,
    value_term text    NOT NULL
);
CREATE INDEX idxissue823
    ON issue823 USING zombodb ((issue823.*));

insert into issue823(value_term)
values ('Bellows Under the bridge');
insert into issue823(value_term)
values ('Burger Meister');
insert into issue823(value_term)
values ('Chemical Smells From Somewhere');
insert into issue823(value_term)
values ('Chowderhead Luncheon');
insert into issue823(value_term)
values ('Green Olive Oyl');
insert into issue823(value_term)
values ('Left Fenders');
insert into issue823(value_term)
values ('Wiper Blade Sandwich Mountain');

SELECT zdb.define_analyzer('case_sensitive_phrase','{"type":"custom", "tokenizer":"standard", "filter":["zdb_truncate_to_fit"]}');

SELECT zdb.define_field_mapping('issue823'::regclass, 'value_term',
                                '{"type": "text", "copy_to": "zdb_all", "analyzer": "phrase", "fielddata": true, "fields":{"exact_case":{"type":"text", "analyzer":"case_sensitive_phrase", "fielddata":true}}}');

REINDEX INDEX idxissue823;

select * from zdb.dump_query('issue823', 'value_term.exact_case:the');
SELECT * FROM issue823 WHERE issue823 ==> 'value_term.exact_case:the';
SELECT upper(term) term, count, term AS exact_term from zdb.tally('issue823'::regclass, 'value_term.exact_case', 'FALSE', '^[cC].*', ''::zdbquery, 5000, 'term'::termsorderby);

DROP TABLE issue823;