set datestyle to 'iso, mdy';

CREATE INDEX idxso_posts ON so_posts USING zombodb (zdb(so_posts)) WITH (url='http://localhost:9200/');
CREATE INDEX idxso_users ON so_users USING zombodb (zdb(so_users)) WITH (url='http://localhost:9200/');
CREATE INDEX idxwords ON words USING zombodb (zdb(words)) WITH (url='http://localhost:9200/');
