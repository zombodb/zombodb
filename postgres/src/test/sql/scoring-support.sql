SET enable_indexscan TO OFF;
SET enable_bitmapscan TO OFF;
SET enable_seqscan TO OFF;

SET enable_indexscan TO ON;
explain select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb('so_posts', ctid) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb('so_posts', ctid) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
SET enable_indexscan TO OFF;

SET enable_bitmapscan TO ON;
explain select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb('so_posts', ctid) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb('so_posts', ctid) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
SET enable_bitmapscan TO OFF;

SET enable_seqscan TO ON;
explain select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb('so_posts', ctid) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
select zdb_score('so_posts', ctid) IS NOT NULL, id, title from so_posts where zdb('so_posts', ctid) ==> 'beer and title:*' order by zdb_score('so_posts', ctid) desc, id;
SET enable_seqscan TO OFF;

