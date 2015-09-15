set enable_indexscan to off;
set enable_bitmapscan to off;

explain (costs off) select id, title from so_posts where zdb('so_posts', ctid) ==> 'beer' order by id;
select id, title from so_posts where zdb('so_posts', ctid) ==> 'beer' order by id;