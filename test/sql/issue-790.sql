CREATE TABLE issue790 AS SELECT 'The map described sailing 23° North by 17° East to reach the island.
The name of the island is ⅋☺︎＠¡¿
To retrieve the treasure, find the largest tree behind BestBuy® and
click the treasure chest icon.' AS t;
CREATE INDEX idxissue790 ON issue790 USING zombodb ((issue790.*));

SELECT * FROM issue790 WHERE issue790 ==> 't: (map w/2 sailing w/25 island)';
SELECT * FROM issue790 WHERE issue790 ==> 't: ( (map w/2 sailing) w/25 island)';
SELECT * FROM issue790 WHERE issue790 ==> 't: (map w/2 (sailing w/25 island))';
SELECT * FROM issue790 WHERE issue790 ==> 't: ("Island" w/25 "Map" w/2 "Sailing")';
SELECT * FROM issue790 WHERE issue790 ==> 't: ( "island" w/25 (("Map" w/2 "Sailing") OR ("Find" w/2 "Largest")))';

DROP TABLE issue790;