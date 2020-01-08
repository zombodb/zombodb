CREATE TABLE cars (model varchar, id bigint);
INSERT INTO cars (model, id) VALUES ('ford', 1);
CREATE INDEX idx_cars ON cars USING zombodb ((cars.*));
SELECT * FROM cars WHERE cars ==> dsl.offset(0, 'ford');
SELECT * FROM cars WHERE cars ==> dsl.offset(1, 'ford');
SELECT * FROM cars WHERE cars ==> dsl.offset(2, 'ford');

DROP TABLE cars;
