alter index idxso_posts set (options='user_data:(owner_user_id=<public.so_users.idxso_users>id), comment_data:(id=<public.so_comments.idxso_comments>post_id)');
select count(*) from so_posts where so_posts ==> 'user_data.display_name:j* and comment_data.user_display_name:j*';
alter index idxso_posts reset (options);