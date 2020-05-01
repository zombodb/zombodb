CREATE TABLE no_hot AS SELECT * FROM events WHERE id = 1;
UPDATE no_hot SET id = id WHERE id = 1;
CREATE INDEX idxno_hot ON no_hot USING zombodb ((no_hot.*));
DROP TABLE no_hot CASCADE;