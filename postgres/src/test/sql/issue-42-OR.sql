SELECT assert(count(*), 165240, 'issue-42-OR') FROM so_posts WHERE zdb(so_posts) ==> 'last_editor_display_name<>Dan OR last_editor_display_name<>Ashwin';
