CREATE TABLE cats(name text);
INSERT INTO cats VALUES ('a'), ('b'), ('c');

CREATE TYPE cat_idx_type AS (name text);

CREATE OR REPLACE FUNCTION cat_idx_func(text) RETURNS cat_idx_type IMMUTABLE STRICT PARALLEL SAFE LANGUAGE sql AS $$
SELECT ROW($1)::cat_idx_type;
$$;

CREATE INDEX cats_idx ON cats USING zombodb (cat_idx_func(name)) WITH (url='http://localhost:9200/');

EXPLAIN
SELECT * FROM cats where cat_idx_func(name) ==> dsl.row_estimate(2,'cat');

DROP TABLE cats;
DROP TYPE cat_idx_type CASCADE;
