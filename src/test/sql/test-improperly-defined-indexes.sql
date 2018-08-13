CREATE TABLE bad_index (
    id int,
    title text
);

CREATE INDEX idxbad_index ON bad_index USING zombodb (id, title);
CREATE INDEX idxbad_index ON bad_index USING zombodb (id);
CREATE INDEX idxbad_index ON bad_index USING zombodb (to_ascii(title));

DROP TABLE bad_index CASCADE;
