CREATE TABLE issue574 (t zdb.fulltext_with_shingles);
CREATE INDEX idxissue574 ON issue574 USING zombodb ((issue574.*));
SELECT * FROM zdb.highlight_document('issue574', '{"t": "hi there"}'::jsonb, 't:"hi there"');
DROP TABLE issue574;