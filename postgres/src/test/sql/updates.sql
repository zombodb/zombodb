VACUUM so_posts;

SELECT assert(count(*), 165240, '# of posts') FROM so_posts;

BEGIN;
UPDATE so_posts SET id = id WHERE id IN (SELECT id FROM so_posts ORDER BY random() LIMIT 1000);
SELECT assert(count(*), 165240, '# of posts') FROM so_posts;
SELECT assert(zdb_estimate_count('so_posts', ''), 1000 + (SELECT count(*) FROM so_posts), 'estimated count');
COMMIT;

SELECT assert(zdb_estimate_count('so_posts', ''), (SELECT count(*) FROM so_posts), 'estimated count');

BEGIN;
UPDATE so_posts SET id = id WHERE id IN (SELECT id FROM so_posts ORDER BY random() LIMIT 1000);
SELECT assert(count(*), 165240, '# of posts') FROM so_posts;
SELECT assert(zdb_estimate_count('so_posts', ''), 1000 + (SELECT count(*) FROM so_posts), 'estimated count');
ABORT;

SELECT assert(zdb_estimate_count('so_posts', ''), (SELECT count(*) FROM so_posts), 'estimated count');

