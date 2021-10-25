alter index idxso_posts set (options='user_data:(owner_user_id=<public.so_users.idxso_users>id)');
alter index idxso_posts set (max_terms_count = 10131072);
select zdb.count('so_posts', 'not user_data.id:-1');
alter index idxso_posts reset(options);
alter index idxso_posts reset(max_terms_count);
