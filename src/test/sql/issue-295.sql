CREATE TABLE rvs (
  id                BIGSERIAL NOT NULL    CONSTRAINT pk_rvs PRIMARY KEY,
  txt               TEXT
);

CREATE INDEX zombo_rvs
  ON rvs
  USING zombodb ((rvs.*));


INSERT into rvs (txt) VALUES ('Lorem ipsum dolor'),
                             ('enim ipsam voluptatem '),
                             ('At vero eos et accusamus ');

set enable_indexscan to off;
set enable_bitmapscan to off;
SELECT COUNT(*)
FROM rvs
WHERE (rvs ==> ('{"match": { "txt": { "query": "ipsum", "fuzziness": "AUTO", "operator": "and" } } }'));

drop table rvs;