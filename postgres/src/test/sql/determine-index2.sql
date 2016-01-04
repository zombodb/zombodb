
CREATE TABLE public.mam_doc (
  pk_mam_doc SERIAL8,
  security   TEXT,
  CONSTRAINT idx_mam_doc PRIMARY KEY (pk_mam_doc)
);
CREATE TABLE public.mam_doc_sub (
  pk_sub     SERIAL8,
  fk_mam_doc BIGINT,
  security   TEXT,
  CONSTRAINT idx_mam_doc_sub PRIMARY KEY (pk_sub)
);
INSERT INTO mam_doc (security) VALUES ('eric');
INSERT INTO mam_doc_sub (fk_mam_doc, security) VALUES (1, 'mark');
CREATE INDEX es_sub ON mam_doc_sub USING zombodb (zdb('public.mam_doc_sub':: REGCLASS, ctid), zdb(mam_doc_sub.*)) WITH (url='http://localhost:9200/', replicas=1, shards=5);
CREATE INDEX es_mam_doc ON mam_doc USING zombodb (zdb('public.mam_doc':: REGCLASS, ctid), zdb(mam_doc.*)) WITH (url='http://localhost:9200/', replicas=1, shards=5);
CREATE OR REPLACE FUNCTION zdb_mam_doc_to_sub(table_name REGCLASS, ctid tid)
  RETURNS tid AS '$libdir/plugins/zombodb', 'zdb_table_ref_and_tid'
LANGUAGE C IMMUTABLE STRICT
COST 1;
ALTER FUNCTION zdb_mam_doc_to_sub( REGCLASS, tid )
OWNER TO cvuser;
CREATE INDEX es_idx_zdb_mam_doc_to_sub ON public.mam_doc USING zombodb (zdb_mam_doc_to_sub('public.mam_doc', ctid), zdb(mam_doc.*)) WITH (shadow='es_mam_doc', OPTIONS ='mam_doc_sub_data:(pk_mam_doc=<mam_doc_sub.es_sub>fk_mam_doc)');

CREATE VIEW mam_doc_test AS
  SELECT
    mam_doc.*,
    (SELECT json_agg(row_to_json(cp.*)) AS json_agg
     FROM (SELECT mam_doc_sub.*
           FROM mam_doc_sub
           WHERE mam_doc_sub.fk_mam_doc = mam_doc.pk_mam_doc) cp) AS mam_doc_sub_data,
    zdb_mam_doc_to_sub('public.mam_doc', ctid)                    AS zdb
  FROM public.mam_doc;

SELECT assert(zdb_determine_index('mam_doc_test')::regclass, 'es_idx_zdb_mam_doc_to_sub'::regclass, 'picked correct index');

DROP TABLE mam_doc CASCADE;
DROP TABLE mam_doc_sub CASCADE;