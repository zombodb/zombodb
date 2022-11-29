CREATE TABLE test_date_time AS
SELECT id, last_activity_date::date date, last_activity_date::time time
FROM so_posts
WHERE last_activity_date IS NOT NULL;
CREATE INDEX idxtest_date_time ON test_date_time USING zombodb ((test_date_time.*));

select * from zdb.terms('test_date_time', 'date', '') order by 2, 1;
select * from zdb.terms('test_date_time', 'time', '') order by 2, 1;

DROP TABLE test_date_time;