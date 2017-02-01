alter index idxso_posts set (always_resolve_joins=true, options='user_data:(owner_user_id=<so_users.idxso_users>id), comment_data:(id=<so_comments.idxso_comments>post_id)');
select count(*) from so_posts where zdb('so_posts', ctid) ==> 'user_data.display_name:j* and comment_data.user_display_name:j*';
alter index idxso_posts reset (options, always_resolve_joins);