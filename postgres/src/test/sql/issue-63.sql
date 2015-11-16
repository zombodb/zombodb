CREATE TABLE tas_update_fail (
  pk_id           SERIAL8,
  start_date_text TEXT,
  end_date_text   TEXT,
  duration        TEXT,
  CONSTRAINT tas_update_fail_pkey PRIMARY KEY (pk_id)
);

INSERT INTO tas_update_fail (start_date_text, end_date_text) VALUES ('1/1/1999', '12/31/1999');
INSERT INTO tas_update_fail (start_date_text, end_date_text) VALUES ('1/1/1999', '2/3/1999');
INSERT INTO tas_update_fail (start_date_text, end_date_text) VALUES ('12/1/1999', '12/31/1999');
INSERT INTO tas_update_fail (start_date_text, end_date_text) VALUES ('2/5/2015', '12/31/2016');
INSERT INTO tas_update_fail (start_date_text, end_date_text) VALUES ('1/1/1999', 'UNKNOWN');

-- Function: isdate(text)
-- DROP FUNCTION isdate(text);

CREATE OR REPLACE FUNCTION isdate(TEXT)
  RETURNS INTEGER AS $BODY$ BEGIN IF ($1 IS NULL)
THEN RETURN 0; END IF;
  PERFORM $1 :: DATE;
  RETURN 1;
  EXCEPTION WHEN OTHERS THEN RETURN 0; END; $BODY$ LANGUAGE plpgsql VOLATILE COST 100;

SELECT
  *,
  isdate(start_date_text),
  isdate(end_date_text)
FROM tas_update_fail ORDER BY pk_id;

CREATE INDEX es_idx_tas_update_fail ON tas_update_fail USING zombodb (zdb('tas_update_fail', ctid), zdb(tas_update_fail.*)) WITH (url='http://localhost:9200/', shards=2, replicas=1);

SELECT *
FROM zdb_tally('tas_update_fail', 'end_date_text', '0', '^.*', '', 5000, 'term' :: zdb_tally_order);

UPDATE tas_update_fail
SET duration = CASE WHEN isdate(end_date_text) = 1 AND isdate(start_date_text) = 1
  THEN (end_date_text :: DATE - start_date_text :: DATE) :: TEXT
               ELSE NULL END;

SELECT *
FROM zdb_tally('tas_update_fail', 'end_date_text', '0', '^.*', '', 5000, 'term' :: zdb_tally_order);

DROP TABLE tas_update_fail CASCADE;
DROP FUNCTION isdate(TEXT);