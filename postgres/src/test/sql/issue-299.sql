-- don't highlight 'Roger' in "user_display_name"
select * from zdb_highlight('so_comments', 'id:1541 or user_display_name = "Roger"', 'id=1541');

-- highlight the entire value of 'Roger Pate' in "user_display_name"
select * from zdb_highlight('so_comments', 'id:1541 or user_display_name = "Roger Pate"', 'id=1541');