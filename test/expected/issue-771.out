CREATE TABLE issue771();
CREATE INDEX idxissue771 ON issue771 USING zombodb ((issue771.*)) WITH (url='http://localhost:9200/');
CREATE FUNCTION issue771_shadow (anyelement) RETURNS anyelement IMMUTABLE STRICT PARALLEL SAFE LANGUAGE c AS '$libdir/zombodb.so', 'shadow_wrapper';
CREATE INDEX idxissue771_shadow ON issue771 USING zombodb (issue771_shadow(issue771.*)) with (shadow=true);
DROP TABLE issue771;
DROP FUNCTION issue771_shadow;
