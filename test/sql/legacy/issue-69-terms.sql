SELECT *
FROM zdb.tally('idxso_posts', 'title', '^a.*', '', 5000, 'term')
LIMIT 10;
