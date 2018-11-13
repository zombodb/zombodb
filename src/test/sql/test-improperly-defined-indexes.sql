CREATE TABLE bad_index (
    id int,
    title text
);

CREATE INDEX idxbad_index ON bad_index USING zombodb (id, title);
CREATE INDEX idxbad_index ON bad_index USING zombodb (id);
CREATE INDEX idxbad_index ON bad_index USING zombodb (to_ascii(title));
CREATE INDEX idxbad_index ON bad_index USING zombodb((bad_index.*)) WHERE id > 0;

CREATE INDEX idxgood_index ON bad_index USING zombodb((bad_index.*));
CREATE INDEX idxsecond_index ON bad_index USING zombodb((bad_index.*));

DROP TABLE bad_index CASCADE;
