SELECT *
FROM zdb_tally('so_posts', 'title', '^a.*', '', 5000, 'term')
LIMIT 10;
