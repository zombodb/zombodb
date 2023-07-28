UPDATE so_posts SET id = id;
VACUUM (PARALLEL 8) so_posts;

SELECT zdb.count('idxso_posts', match_all()),
       zdb.raw_count('idxso_posts', match_all()),
       zdb.count('idxso_posts', match_all())+1 = zdb.raw_count('idxso_posts', match_all());