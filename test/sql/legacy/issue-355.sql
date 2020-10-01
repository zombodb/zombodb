CREATE TABLE issue355 (
   id serial8,
   name text,
   label zdb.phrase,
   description zdb.fulltext
);

CREATE INDEX idxissue355 ON issue355 USING zombodb( (issue355.*) ) WITH (field_lists='name=[name, label, description]');

SELECT * FROM zdb.tally('idxissue355', 'name', '^.*', '', 10, 'reverse_count');

DROP TABLE issue355;