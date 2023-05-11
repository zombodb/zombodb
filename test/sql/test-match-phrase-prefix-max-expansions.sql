select count(*) from so_posts where so_posts ==> 'body: "java is*"';
select count(*) from so_posts where so_posts ==> '({
 "match_phrase_prefix": {
   "body": {
     "query": "java is",
     "boost": 1.0,
     "max_expansions": 2147483647
   }
 }
})';
select zdb.dump_query('so_posts', 'body: "java is*"');

select count(*) from so_posts where so_posts ==> 'body: "java i*"';
select count(*) from so_posts where so_posts ==> '({
 "match_phrase_prefix": {
   "body": {
     "query": "java i",
     "boost": 1.0,
     "max_expansions": 2147483647
   }
 }
})';
select zdb.dump_query('so_posts', 'body: "java i*"');