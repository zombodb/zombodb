set datestyle to 'iso, mdy';

SELECT assert('# of posts', 165240, count(*)) FROM so_posts;

BEGIN;
UPDATE so_posts SET id = id WHERE id IN (SELECT id FROM so_posts ORDER BY random() LIMIT 1000);
SELECT assert('# of posts', 165240, count(*)) FROM so_posts;
SELECT assert('estimated count', 1000 + (SELECT count(*) FROM so_posts), zdb_estimate_count('so_posts', ''));
COMMIT;

SELECT assert('estimated count', (SELECT count(*) FROM so_posts), zdb_estimate_count('so_posts', ''));

BEGIN;
UPDATE so_posts SET id = id WHERE id IN (SELECT id FROM so_posts ORDER BY random() LIMIT 1000);
SELECT assert('# of posts', 165240, count(*)) FROM so_posts;
SELECT assert('estimated count', 1000 + (SELECT count(*) FROM so_posts), zdb_estimate_count('so_posts', ''));
ABORT;

SELECT assert('estimated count', (SELECT count(*) FROM so_posts), zdb_estimate_count('so_posts', ''));

