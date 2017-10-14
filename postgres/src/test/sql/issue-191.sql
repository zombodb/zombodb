CREATE TABLE "Issue191" (
  "ID"               SERIAL8 NOT NULL PRIMARY KEY,
  "BobIsYourUncle" TEXT
);

CREATE VIEW "Issue191_View" AS SELECT *, zdb('"Issue191"', ctid) FROM "Issue191";

CREATE INDEX idx_issue191
  ON "Issue191" USING zombodb (zdb('"Issue191"', ctid), zdb("Issue191")) WITH (url='http://localhost:9200/');

INSERT INTO "Issue191" ("BobIsYourUncle") VALUES ('abc');
INSERT INTO "Issue191" ("BobIsYourUncle") VALUES ('def');

--
-- one field from each of table and view
--
SELECT table_name, user_identifier, query, total, row_data FROM zdb_multi_search(ARRAY ['public."Issue191"'], NULL, ARRAY [ARRAY['BobIsYourUncle']], '');
SELECT table_name, user_identifier, query, total, row_data FROM zdb_multi_search(ARRAY ['public."Issue191_View"'], NULL, ARRAY [ARRAY['BobIsYourUncle']], '');

--
-- two fields from each of table and view
--
SELECT table_name, user_identifier, query, total, row_data FROM zdb_multi_search(ARRAY ['public."Issue191"'], NULL, ARRAY [ARRAY['ID', 'BobIsYourUncle']], '');
SELECT table_name, user_identifier, query, total, row_data FROM zdb_multi_search(ARRAY ['public."Issue191_View"'], NULL, ARRAY [ARRAY['ID', 'BobIsYourUncle']], '');

--
-- all fields from each of table and view
--
SELECT table_name, user_identifier, query, total, row_data FROM zdb_multi_search(ARRAY ['public."Issue191"'], NULL, NULL, '');
SELECT table_name, user_identifier, query, total, row_data FROM zdb_multi_search(ARRAY ['public."Issue191_View"'], NULL, NULL, '');

DROP TABLE "Issue191" CASCADE;