CREATE TABLE issue606 (id serial8, b boolean, t text);
CREATE INDEX idxissue606 ON issue606 USING zombodb ((issue606.*));

INSERT INTO issue606 (b, t) VALUES (true, 'TRUE');
INSERT INTO issue606 (b, t) VALUES (false, 'FALSE');

SELECT * FROM issue606 WHERE issue606 ==> 'b: TRUE';
SELECT * FROM issue606 WHERE issue606 ==> 'b: FALSE';

SELECT * FROM issue606 WHERE issue606 ==> 't: "TRUE"';
SELECT * FROM issue606 WHERE issue606 ==> 't: "FALSE"';

DROP TABLE issue606;