SELECT * FROM zdb_range_agg('so_posts', 'favorite_count', '[{"from":10},{"key":"bob","from":100,"to":200},{"to":1000}]', '') ORDER BY doc_count DESC;
