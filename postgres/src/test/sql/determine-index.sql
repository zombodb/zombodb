CREATE VIEW so_posts_view_without_score AS SELECT ctid, *, zdb('so_posts', ctid) zdb FROM so_posts;
CREATE VIEW so_posts_view_with_score AS SELECT ctid, zdb_score('so_posts', ctid), *, zdb('so_posts', ctid) zdb FROM so_posts;
CREATE VIEW so_posts_view_no_zdb AS SELECT * FROM so_posts;

SELECT zdb_determine_index('so_posts')::regclass;
SELECT zdb_determine_index('so_posts_view_without_score')::regclass;
SELECT zdb_determine_index('so_posts_view_with_score')::regclass;
SELECT zdb_determine_index('so_posts_view_no_zdb')::regclass;

SELECT zdb_score('so_posts_view_without_score', ctid) IS NOT NULL, id FROM so_posts_view_without_score WHERE zdb ==> 'beer and title:*' ORDER BY id;
SELECT zdb_score IS NOT NULL, zdb_score('so_posts_view_with_score', ctid) IS NOT NULL, id FROM so_posts_view_with_score WHERE zdb ==> 'beer and title:*' ORDER BY id;

DROP VIEW so_posts_view_without_score;
DROP VIEW so_posts_view_with_score;
DROP VIEW so_posts_view_no_zdb;