CREATE TABLE issue253 (
  id serial8,
  geom postgis.geometry
);

INSERT INTO issue253 (geom) VALUES ('0101000020E610000087A19B1E104053C079CE71C9CC7F4340');

CREATE INDEX idxissue253 ON issue253 USING zombodb ((issue253.*));

SELECT * FROM issue253 WHERE issue253 ==> dsl.geo_shape('geom', '{"type":"Point","coordinates":[-77.00098386,38.9984371]}', 'INTERSECTS');
SELECT * FROM issue253 WHERE issue253 ==> dsl.geo_shape('geom', '{"type":"Point","coordinates":[-77.00098386,38.9984371]}', 'CONTAINS');
SELECT * FROM issue253 WHERE issue253 ==> dsl.geo_shape('geom', '{"type":"Point","coordinates":[-77.00098386,38.9984371]}', 'WITHIN');
SELECT * FROM issue253 WHERE issue253 ==> dsl.geo_shape('geom', '{"type":"Point","coordinates":[-77.00098386,38.9984371]}', 'DISJOINT');

DROP TABLE issue253;