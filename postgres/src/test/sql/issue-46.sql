SELECT * FROM zdb_range_agg('so_posts', 'closed_date', '[ {"from":"2010-12-01T00:00:00-04"}, {"key":"foo", "from":"2010-01-01T00:00:00-04", "to":"2012-12-31T00:00:00-04"}, {"to":"2010-12-14T00:00:00-04"} ]', '') ORDER BY doc_count DESC;
