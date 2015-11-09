SELECT * FROM zdb_highlight(
    'so_posts',
    '( title:"* non * programmers" )',
    'id IN (1,4,9)',
    '{"title"}'::TEXT[])
ORDER BY "primaryKey", "fieldName", "arrayIndex", "position";