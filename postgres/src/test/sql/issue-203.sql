select id, title from so_posts where zdb('so_posts', ctid) ==> '#limit(id asc, 0, 10) java and title:*' order by 1 asc;
select count(*) from so_posts where zdb('so_posts', ctid) ==> '#limit(id desc, 0, 10) beer and title:*';

select id from so_posts where zdb('so_posts', ctid) ==> '#limit(id asc, 0, 10) java and title:*' order by 1 asc;
select id from so_posts where zdb('so_posts', ctid) ==> '#limit(id asc, 10, 10) java and title:*' order by 1 asc;
