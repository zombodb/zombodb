select * from zdb.tally('idxso_posts', 'last_activity_date', 'week', 'cheese', 5000, 'term');
select * from zdb.tally('idxso_posts', 'last_activity_date', 'week:-1d', 'cheese', 5000, 'term');