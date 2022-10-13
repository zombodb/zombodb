CREATE TABLE issue770_without (
    ts time without time zone
);
CREATE INDEX idxissue770 ON issue770_without USING zombodb ((issue770_without.*));

INSERT INTO issue770_without (ts) VALUES ('15:00');
INSERT INTO issue770_without (ts) VALUES ('15:00:00');
INSERT INTO issue770_without (ts) VALUES ('15:00:00.0');
INSERT INTO issue770_without (ts) VALUES ('15:00:00.00');
INSERT INTO issue770_without (ts) VALUES ('15:00:00.0000');
INSERT INTO issue770_without (ts) VALUES ('15:00:00.00000');
INSERT INTO issue770_without (ts) VALUES ('15:00:00.000000');

SELECT zdb.reapply_mapping('issue770_without');
DROP TABLE issue770_without;

CREATE TABLE issue770_with (
    ts time with time zone
);
CREATE INDEX idxissue770 ON issue770_with USING zombodb ((issue770_with.*));

INSERT INTO issue770_with (ts) VALUES ('15:00');
INSERT INTO issue770_with (ts) VALUES ('15:00:00');
INSERT INTO issue770_with (ts) VALUES ('15:00:00.0');
INSERT INTO issue770_with (ts) VALUES ('15:00:00.00');
INSERT INTO issue770_with (ts) VALUES ('15:00:00.0000');
INSERT INTO issue770_with (ts) VALUES ('15:00:00.00000');
INSERT INTO issue770_with (ts) VALUES ('15:00:00.000000');
SELECT zdb.reapply_mapping('issue770_with');

DROP TABLE issue770_with;

