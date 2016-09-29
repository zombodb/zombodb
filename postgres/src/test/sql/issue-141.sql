SELECT *
FROM zdb_highlight('so_posts', 'body:("coworker" w/4 ("that" OR "this"))', 'id=1', '{body}')
ORDER BY "primaryKey", "fieldName", "arrayIndex", "position";