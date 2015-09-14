SELECT assert(count(*), 165238, 'issue-42-AND') FROM so_posts WHERE zdb('so_posts', ctid) ==> 'last_editor_display_name<>Dan AND last_editor_display_name<>Ashwin';
