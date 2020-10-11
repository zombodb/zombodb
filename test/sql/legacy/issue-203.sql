select count(*) from so_posts where so_posts ==> dsl.limit(10, dsl.sort('id', 'asc', 'java and title:*'));
select count(*) from so_posts where so_posts ==> dsl.limit(10, dsl.sort('_score', 'desc', 'beer and title:*'));

select id from so_posts where so_posts ==> dsl.limit(10, dsl.sort('id', 'asc', 'java and title:*')) order by 1 asc;
select id from so_posts where so_posts ==> dsl.limit(10, dsl.offset(10, dsl.sort('id', 'asc', 'java and title:*'))) order by 1 asc;
