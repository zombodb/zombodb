CREATE TABLE test_index_all_types (
 bytea                        bytea,
 boolean                      boolean,
 smallint                     smallint,
 integer                      integer,
 bigint                       bigint,
 real                         real,
 double_precision             double precision,
 character_varying            varchar(50),
 text                         text,
-- citext                       citext,
 time_without_time_zone       time without time zone,
 time_with_time_zone          time with time zone,
 date                         date,
 timestamp_without_time_zone  timestamp without time zone,
 timestamp_with_time_zone     timestamp with time zone,
 json                         json,
 jsonb                        jsonb,
 inet                         inet,
 fulltext                     zdb.fulltext
);

INSERT INTO test_index_all_types VALUES (
  'bytea'::bytea,
  true,
  1,
  1,
  1,
  1.1111,
  1.1111,
  'varchar(50)',
  'text',
  now()::time without time zone,
  now()::time with time zone,
  now()::date,
  now()::timestamp without time zone,
  now()::timestamp with time zone,
  '{"json":"data"}',
  '{"json":"data"}',
  '127.0.0.1',
  'fulltext'
);

CREATE INDEX idxtest_index_all_types ON test_index_all_types USING zombodb ((test_index_all_types));

SELECT zdb.count('idxtest_index_all_types', '');

DROP TABLE test_index_all_types CASCADE;