SELECT assert(count(*), 310546, 'count(*) from words') FROM words;
SELECT assert(count(*), 310546, 'count(word:*) from words') FROM words WHERE zdb(words) ==> 'word:*';
SELECT assert(zdb_estimate_count('words', 'word:*'), 310546, 'word:* estimate from words');

SELECT assert(count(*), 165240, 'count(*) from so_posts') FROM so_posts;
SELECT assert(count(*), 165240, 'count(id:*) from so_posts') FROM so_posts WHERE zdb(so_posts) ==> 'id:*';
SELECT assert(zdb_estimate_count('so_posts', 'id:*'), 165240, 'id:* estimate from so_posts');

SELECT assert(count(*), 136990, 'count(*) from so_users') FROM so_users;
SELECT assert(count(*), 136990, 'count(id:*) from so_users') FROM so_users WHERE zdb(so_users) ==> 'id:*';
SELECT assert(zdb_estimate_count('so_users', 'id:*'), 136990, 'id:* estimate from so_users');



