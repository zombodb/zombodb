set enable_indexscan to off;
set enable_bitmapscan to off;

explain (costs off) select id, title from so_posts where zdb(so_posts) ==> 'beer' order by id;
select id, title from so_posts where zdb(so_posts) ==> 'beer' order by id;

explain (costs off) select so_posts.title, so_comments.user_display_name from so_posts inner join so_comments on so_posts.id = so_comments.post_id where zdb(so_posts) ==> 'beer' order by so_posts.id;
select so_posts.title, so_comments.user_display_name from so_posts inner join so_comments on so_posts.id = so_comments.post_id where zdb(so_posts) ==> 'beer' order by so_posts.id;

explain (costs off) select id from so_posts where zdb(so_posts) ==> 'beer' or zdb(so_posts) ==> 'wine';
select id from so_posts where zdb(so_posts) ==> 'beer' or zdb(so_posts) ==> 'wine' order by id;

explain (costs off) select id from so_posts where id in (select generate_series(1, 10000)) and zdb(so_posts) ==> 'java' order by id;
select id from so_posts where id in (select generate_series(1, 10000)) and zdb(so_posts) ==> 'java' order by id;

explain (costs off) select count(*) from (with words as (select word from words order by ctid offset 10 limit 50) select id from so_posts, words where zdb(so_posts) ==> words.word) x;
select count(*) from (with words as (select word from words order by ctid offset 10 limit 50) select id from so_posts, words where zdb(so_posts) ==> words.word) x;

explain (costs off) select so_posts.title, so_comments.user_display_name from so_posts inner join so_comments on so_posts.id = so_comments.post_id where zdb(so_posts) ==> 'java' and zdb(so_comments) ==> 'java' order by so_posts.id;
select count(*) FROM (select so_posts.title, so_comments.user_display_name from so_posts inner join so_comments on so_posts.id = so_comments.post_id where zdb(so_posts) ==> 'java' and zdb(so_comments) ==> 'java' order by so_posts.id) x;

set enable_indexscan to on;  set enable_bitmapscan to off; set enable_seqscan to off; select count(*) FROM (select so_posts.title, so_comments.user_display_name from so_posts inner join so_comments on so_posts.id = so_comments.post_id where zdb(so_posts) ==> 'java' and zdb(so_comments) ==> 'java' order by so_posts.id) x;
set enable_indexscan to off; set enable_bitmapscan to on;  set enable_seqscan to off; select count(*) FROM (select so_posts.title, so_comments.user_display_name from so_posts inner join so_comments on so_posts.id = so_comments.post_id where zdb(so_posts) ==> 'java' and zdb(so_comments) ==> 'java' order by so_posts.id) x;
set enable_indexscan to off; set enable_bitmapscan to off; set enable_seqscan to on;  select count(*) FROM (select so_posts.title, so_comments.user_display_name from so_posts inner join so_comments on so_posts.id = so_comments.post_id where zdb(so_posts) ==> 'java' and zdb(so_comments) ==> 'java' order by so_posts.id) x;

