CREATE TABLE public.documents (
  pk_documents SERIAL8,
  doc_stuff    TEXT,
  CONSTRAINT idx_documents PRIMARY KEY (pk_documents)
);
CREATE TABLE public.docs_usage (
  pk_docs_usage      SERIAL8,
  fk_documents       BIGINT,
  fk_library_profile BIGINT,
  place_used         TEXT,
  CONSTRAINT idx_docs_usage PRIMARY KEY (pk_docs_usage)
);
CREATE TABLE public.library_profile (
  pk_library_profile SERIAL8,
  library_name       TEXT,
  CONSTRAINT idx_library_profile PRIMARY KEY (pk_library_profile)
);

CREATE INDEX es_documents ON documents USING zombodb (zdb('public.documents':: REGCLASS, ctid), zdb_to_jsonb(documents.*)) WITH (url='http://localhost:9200/', replicas=1, shards=5);
CREATE INDEX es_docs_usage ON docs_usage USING zombodb (zdb('public.docs_usage':: REGCLASS, ctid), zdb_to_jsonb(docs_usage.*)) WITH (url='http://localhost:9200/', replicas=1, shards=5);
CREATE INDEX es_library_profile ON library_profile USING zombodb (zdb('public.library_profile':: REGCLASS, ctid), zdb_to_jsonb(library_profile.*)) WITH (url='http://localhost:9200/', replicas=1, shards=5);

CREATE OR REPLACE FUNCTION zdb_to_docs_usage(table_name REGCLASS, ctid tid)
  RETURNS tid AS '$libdir/plugins/zombodb', 'zdb_table_ref_and_tid' LANGUAGE C IMMUTABLE STRICT COST 1;

CREATE INDEX es_idx_zdb_to_docs_usage
ON public.documents
USING zombodb (zdb_to_docs_usage('public.documents', ctid), zdb_to_jsonb(documents.*))
WITH (shadow='es_documents',
  options='
            docs_usage_data:(pk_documents=<docs_usage.es_docs_usage>fk_documents),
            fk_library_profile=<library_profile.es_library_profile>pk_library_profile
           '
);

CREATE OR REPLACE VIEW documents_master_view AS
  SELECT
    documents.*,
    (SELECT json_agg(row_to_json(du.*)) AS json_agg
     FROM (SELECT
             docs_usage.*,
             (SELECT library_profile.library_name
              FROM library_profile
              WHERE library_profile.pk_library_profile = docs_usage.fk_library_profile) AS library_name
           FROM docs_usage
           WHERE documents.pk_documents = docs_usage.fk_documents) du) AS usage_data,

    zdb_to_docs_usage('public.documents' :: REGCLASS, documents.ctid)  AS zdb
  FROM public.documents;

INSERT INTO documents (doc_stuff)
VALUES ('Every good boy does fine.'), ('Sally sells sea shells down by the seashore.'),
  ('The quick brown fox jumps over the lazy dog.');
INSERT INTO library_profile (library_name) VALUES ('GSO Public Library'), ('Library of Congress'), ('The interwebs.');
INSERT INTO docs_usage (fk_documents, fk_library_profile, place_used)
VALUES (1, 1, 'somewhere'), (2, 2, 'anywhere'), (3, 3, 'everywhere'), (3, 1, 'somewhere');

SELECT count(*) FROM documents_master_view WHERE public.documents_master_view.zdb ==> 'somewhere';
SELECT count(*) FROM documents_master_view WHERE public.documents_master_view.zdb ==> 'GSO';

DROP TABLE documents CASCADE;
DROP TABLE docs_usage CASCADE;
DROP TABLE library_profile CASCADE;
