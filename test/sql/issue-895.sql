select id, zdb.anyelement_cmpfunc(ctid, 'id:[1,4]'::zdbquery)
from so_posts
where so_posts ==> 'id:[1,2,3,4]'
order by id;

select id, zdb.anyelement_cmpfunc(so_posts, 'id:[1,4]'::zdbquery)
from so_posts
where so_posts ==> 'id:[1,2,3,4]'
order by id;