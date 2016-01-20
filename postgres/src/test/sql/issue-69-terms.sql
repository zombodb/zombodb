SELECT *
FROM zdb_tally('so_posts', 'title', '^A.*', '', 5000, 'term')
LIMIT 10;