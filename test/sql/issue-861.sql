select * from zdb.highlight_document('so_posts', '{"body": "java", "id":42}'::json, 'id:* and body:*') order by 1, 2, 3;
select * from zdb.highlight_document('so_posts', '{"body": "java", "id":42}'::json, 'id:* and body:java') order by 1, 2, 3;
select * from zdb.highlight_document('so_posts', '{"body": "java", "id":42}'::json, 'id:42 and body:*') order by 1, 2, 3;
select * from zdb.highlight_document('so_posts', '{"body": "java", "id":42}'::json, 'id:* or body:*') order by 1, 2, 3;
select * from zdb.highlight_document('so_posts', '{"body": "java", "id":42}'::json, 'id:* or body:java') order by 1, 2, 3;
select * from zdb.highlight_document('so_posts', '{"body": "java", "id":42}'::json, 'id:42 or body:*') order by 1, 2, 3;