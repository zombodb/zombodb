CREATE TABLE b (
  id SERIAL PRIMARY KEY,
  name TEXT
);

CREATE TABLE a (
  id SERIAL PRIMARY KEY,
  b1 INTEGER REFERENCES b (id),
  b2 INTEGER REFERENCES b (id)
);

INSERT INTO b (name) VALUES ('One');
INSERT INTO b (name) VALUES ('Two');
INSERT INTO b (name) VALUES ('Three');
INSERT INTO b (name) VALUES ('Four');
INSERT INTO b (name) VALUES ('Five');

INSERT INTO a (b1, b2) VALUES (1, null);   -- a.id = 1
INSERT INTO a (b1, b2) VALUES (2, null);   -- a.id = 2
INSERT INTO a (b1, b2) VALUES (null, 3);   -- a.id = 3
INSERT INTO a (b1, b2) VALUES (null, 4);   -- a.id = 4
INSERT INTO a (b1, b2) VALUES (5, 5);      -- a.id = 5
INSERT INTO a (b1, b2) VALUES (1, 5);      -- a.id = 6

CREATE INDEX idx_zdb_b
  ON b
  USING zombodb(zdb('b', ctid), zdb(b))
WITH (url = 'http://localhost:9200/');

CREATE INDEX idx_zdb_a
  ON a
  USING zombodb(zdb('a', ctid), zdb(a))
WITH (url = 'http://localhost:9200/',
  options='b1_:(b1=<b.idx_zdb_b>id),
b2_:(b2=<b.idx_zdb_b>id)');

select * from a where zdb('a', ctid) ==> 'b1_.name:one' order by 1;
select * from a where zdb('a', ctid) ==> 'b1_.name:five' order by 1;

select * from a where zdb('a', ctid) ==> 'b2_.name:one' order by 1;
select * from a where zdb('a', ctid) ==> 'b2_.name:five' order by 1;

DROP TABLE a;
DROP TABLE b;