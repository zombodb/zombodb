CREATE INDEX idxevents ON events USING zombodb ((events)) WITH (bulk_concurrency=2, batch_size=2097152);
CREATE INDEX idxusers ON users USING zombodb ((users)) WITH (bulk_concurrency=2, batch_size=2097152);
CREATE INDEX idxso_users ON so_users USING zombodb ((so_users)) WITH (bulk_concurrency=2, batch_size=2097152);
CREATE INDEX idxso_posts ON so_posts USING zombodb ((so_posts)) WITH (bulk_concurrency=2, batch_size=2097152);
CREATE INDEX idxso_comments ON so_comments USING zombodb ((so_comments)) WITH (bulk_concurrency=2, batch_size=2097152);
CREATE INDEX idxproducts ON products USING zombodb ((products)) WITH (bulk_concurrency=2, batch_size=2097152);
CREATE INDEX idxwords ON words USING zombodb ((words)) WITH (bulk_concurrency=2, batch_size=2097152);