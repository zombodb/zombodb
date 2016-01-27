SELECT term
FROM zdb_suggest_terms('so_posts', 'body', 'beer', '', 5000)
LIMIT 10;