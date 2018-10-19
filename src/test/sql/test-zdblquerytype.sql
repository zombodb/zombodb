SELECT NULL::zdbquery;
SELECT ''::zdbquery;
SELECT 'beer'::zdbquery;

SELECT '{"terms":{"subject":"beer"}}'::zdbquery;
SELECT '{"limit":42,"query_dsl":{"terms":{"subject":"beer"}}}'::zdbquery;
SELECT '{"query_dsl":{"terms":{"subject":"beer"}}}'::zdbquery;

SELECT zdb.to_query_dsl(NULL::zdbquery);
SELECT zdb.to_query_dsl(''::zdbquery);
SELECT zdb.to_query_dsl('beer'::zdbquery);
SELECT zdb.to_query_dsl('{"terms":{"subject":"beer"}}'::zdbquery);
SELECT zdb.to_query_dsl('{"limit":42,"query_dsl":{"terms":{"subject":"beer"}}}'::zdbquery);
SELECT zdb.to_query_dsl('{"query_dsl":{"terms":{"subject":"beer"}}}'::zdbquery);

select dsl.row_estimate(88, dsl.min_score(42, dsl.sort('title', 'asc', dsl.offset_limit(10, 42, 'beer'))));