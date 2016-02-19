set datestyle to 'iso, mdy';

CREATE TABLE products (
  id SERIAL8 NOT NULL PRIMARY KEY,
  name text NOT NULL,
  keywords varchar(64)[],
  short_summary phrase,
  long_description fulltext,
  price bigint,
  inventory_count integer,
  discontinued boolean default false,
  availability_date date
);

COPY products FROM PROGRAM 'wget -qO - https://raw.githubusercontent.com/zombodb/zombodb/master/TUTORIAL-data.dmp';

CREATE INDEX idx_zdb_products
ON products
USING zombodb(zdb('products', products.ctid), zdb(products))
WITH (url='http://localhost:9200/');