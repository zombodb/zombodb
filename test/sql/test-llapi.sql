CREATE TABLE llapi (
    id serial8,
    title text,
    ts timestamp default now()
);
CREATE INDEX idxllapi ON llapi USING zombodb ((llapi.*));
SELECT zdb.llapi_direct_insert('idxllapi', '{"field":"this will ERROR"}'::json);

ALTER INDEX idxllapi SET (llapi=true);

INSERT INTO llapi (title) VALUES ('This will ERROR');
CREATE RULE llapi_rule AS ON INSERT TO llapi DO INSTEAD (SELECT zdb.llapi_direct_insert('idxllapi', to_json(NEW)));
INSERT INTO llapi (title) VALUES ('one');
INSERT INTO llapi (title) VALUES ('two');
INSERT INTO llapi (title) VALUES ('three');
INSERT INTO llapi (title) VALUES ('four');
INSERT INTO llapi (title) VALUES ('five');
BEGIN;
INSERT INTO llapi (title) VALUES ('six - aborted');
ABORT;

SELECT * FROM zdb.terms('idxllapi', 'title', dsl.match_all()) ORDER BY term;
VACUUM llapi;
SELECT * FROM zdb.terms('idxllapi', 'title', dsl.match_all()) ORDER BY term;

VACUUM FULL llapi;
SELECT * FROM zdb.terms('idxllapi', 'title', dsl.match_all()) ORDER BY term;

REINDEX INDEX idxllapi;
SELECT * FROM zdb.terms('idxllapi', 'title', dsl.match_all()) ORDER BY term;

DROP TABLE llapi CASCADE;