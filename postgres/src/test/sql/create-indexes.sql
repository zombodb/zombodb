set datestyle to 'iso, mdy';

CREATE INDEX idxso_posts ON so_posts USING zombodb (zdb(so_posts), zdb_to_json(so_posts)) WITH (url='http://localhost:9200/', bulk_concurrency=1, batch_size=1048576);
CREATE INDEX idxso_users ON so_users USING zombodb (zdb(so_users), zdb_to_json(so_users)) WITH (url='http://localhost:9200/', bulk_concurrency=1, batch_size=1048576);
CREATE INDEX idxso_comments ON so_comments USING zombodb(zdb(so_comments), zdb_to_json(so_comments)) WITH (url='http://localhost:9200/', bulk_concurrency=1, batch_size=1048576);
CREATE INDEX idxwords ON words USING zombodb (zdb(words), zdb_to_json(words)) WITH (url='http://localhost:9200/', bulk_concurrency=1, batch_size=1048576);
CREATE INDEX idx_zdb_products ON products USING zombodb(zdb(products), zdb_to_json(products)) WITH (url='http://localhost:9200/');
