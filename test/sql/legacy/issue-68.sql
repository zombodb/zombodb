ALTER INDEX idxso_posts SET (field_lists='title_and_tags=[title,tags]');

SELECT * FROM zdb_highlight('so_posts', 'title_and_tags:java', 'zdb(''so_posts'', ctid)==>''title_and_tags:java'' ORDER BY id LIMIT 10');