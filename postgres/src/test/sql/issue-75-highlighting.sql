SELECT DISTINCT "fieldName"
FROM zdb_highlight('so_posts', '#bool(#must(title:java javascript))',
                   $$ zdb(the_table)==>'#bool(#must(java javascript))' $$) ORDER BY 1;