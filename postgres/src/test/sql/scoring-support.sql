SET enable_indexscan TO OFF;
SET enable_bitmapscan TO OFF;
SET enable_seqscan TO OFF;

SET enable_indexscan TO ON;
explain (costs off) select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb(so_posts) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb(so_posts) ==> 'beer and title:*' order by id, zdb_score('so_posts', ctid) desc;
SET enable_indexscan TO OFF;

SET enable_bitmapscan TO ON;
explain (costs off) select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb(so_posts) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb(so_posts) ==> 'beer and title:*' order by id, zdb_score('so_posts', ctid) desc;
SET enable_bitmapscan TO OFF;

SET enable_seqscan TO ON;
explain (costs off) select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb(so_posts) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb(so_posts) ==> 'beer and title:*' order by id, zdb_score('so_posts', ctid) desc;
SET enable_seqscan TO OFF;

