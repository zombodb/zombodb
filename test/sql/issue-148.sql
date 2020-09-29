CREATE TABLE issue148 (null_field text[]);
INSERT INTO issue148 VALUES (ARRAY['one', null, 'two']);
CREATE INDEX idxissue148 ON issue148 USING zombodb ( (issue148.*));

SELECT * FROM issue148 WHERE issue148 ==> 'null_field:null';
DROP TABLE issue148;