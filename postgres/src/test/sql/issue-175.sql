CREATE TABLE city (
  id        BIGSERIAL NOT NULL PRIMARY KEY,
  city_name VARCHAR(255)
);

INSERT INTO city (city_name) VALUES ('Seattle');
INSERT INTO city (city_name) VALUES ('Washington');

CREATE TABLE client (
  id         BIGSERIAL NOT NULL PRIMARY KEY,
  first_name VARCHAR(255),
  last_name  VARCHAR(255),
  city_id    BIGINT    NOT NULL REFERENCES city (id)
);

INSERT INTO client (first_name, last_name, city_id)
VALUES ('John', 'Smith', 1);

INSERT INTO client (first_name, last_name, city_id)
VALUES ('John', 'Doe', 1);

INSERT INTO client (first_name, last_name, city_id)
VALUES ('John', 'Wick', 2);

CREATE INDEX idxcity
  ON city USING zombodb(zdb('city', city.ctid), zdb(city)) WITH (url='http://localhost:9200/');
CREATE INDEX idxclient
  ON client USING zombodb(zdb('client', client.ctid), zdb(client)) WITH (url='http://localhost:9200/');

ALTER INDEX idxcity SET ( OPTIONS = 'id=<client.idxclient>city_id');
ALTER INDEX idxclient SET ( OPTIONS = 'city_id=<city.idxcity>id');

CREATE VIEW find_me AS
  SELECT
    client.*,
    city.city_name,
    zdb('client', client.ctid) AS zdb
  FROM client
    JOIN city ON client.city_id = city.id;

select * from find_me where zdb ==> 'john' order by id;

select * from find_me where zdb ==> 'seattle' order by id;

select * from find_me where zdb ==> 'john smith' order by id;

select * from find_me where zdb ==> 'smith seattle' order by id;

DROP TABLE city CASCADE;
DROP TABLE client CASCADE;
