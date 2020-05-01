CREATE TABLE subxact_test (
  id           SERIAL8 NOT NULL PRIMARY KEY,
  start_date_text varchar(255),
  end_date_text   varchar(255),
  duration        varchar(255)
);

INSERT INTO subxact_test (start_date_text, end_date_text) VALUES ('1/1/1999', '12/31/1999');
INSERT INTO subxact_test (start_date_text, end_date_text) VALUES ('1/1/1999', '2/3/1999');
INSERT INTO subxact_test (start_date_text, end_date_text) VALUES ('12/1/1999', '12/31/1999');
INSERT INTO subxact_test (start_date_text, end_date_text) VALUES ('2/5/2015', '12/31/2016');
INSERT INTO subxact_test (start_date_text, end_date_text) VALUES ('1/1/1999', 'UNKNOWN');

CREATE OR REPLACE FUNCTION isdate(TEXT) RETURNS integer AS $$
BEGIN
    IF ($1 IS NULL) THEN
        RETURN 0;
    END IF;

    PERFORM $1::DATE;
    RETURN 1;
  EXCEPTION WHEN invalid_datetime_format THEN
      RETURN 0;
  END;
$$ LANGUAGE plpgsql VOLATILE COST 100;

SELECT
  *,
  isdate(start_date_text),
  isdate(end_date_text)
FROM subxact_test ORDER BY id;

CREATE INDEX idxsubxact_test ON subxact_test USING zombodb ((subxact_test));

SELECT * FROM zdb.terms('idxsubxact_test', 'end_date_text', match_all());

UPDATE subxact_test
SET duration = CASE WHEN isdate(end_date_text) = 1 AND isdate(start_date_text) = 1
  THEN (end_date_text :: DATE - start_date_text :: DATE) :: TEXT
               ELSE NULL END;

SELECT * FROM zdb.terms('idxsubxact_test', 'end_date_text', match_all());

BEGIN;
UPDATE subxact_test SET duration = CASE WHEN isdate(end_date_text) = 1 AND isdate(start_date_text) = 1 THEN (end_date_text::date - start_date_text::date)::text ELSE NULL END WHERE id = 1;
UPDATE subxact_test SET duration = CASE WHEN isdate(end_date_text) = 1 AND isdate(start_date_text) = 1 THEN (end_date_text::date - start_date_text::date)::text ELSE NULL END WHERE id = 2;
UPDATE subxact_test SET duration = CASE WHEN isdate(end_date_text) = 1 AND isdate(start_date_text) = 1 THEN (end_date_text::date - start_date_text::date)::text ELSE NULL END WHERE id = 3;
UPDATE subxact_test SET duration = CASE WHEN isdate(end_date_text) = 1 AND isdate(start_date_text) = 1 THEN (end_date_text::date - start_date_text::date)::text ELSE NULL END WHERE id = 4;
UPDATE subxact_test SET duration = CASE WHEN isdate(end_date_text) = 1 AND isdate(start_date_text) = 1 THEN (end_date_text::date - start_date_text::date)::text ELSE NULL END WHERE id = 5;
COMMIT;

SELECT * FROM zdb.terms('idxsubxact_test', 'end_date_text', match_all());

DROP TABLE subxact_test CASCADE;
DROP FUNCTION isdate(TEXT);