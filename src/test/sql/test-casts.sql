SELECT pg_typeof('beer'::text::zdbquery);
SELECT pg_typeof('{"term":"beer"}'::json::zdbquery);
SELECT pg_typeof('{"term":"beer"}'::jsonb::zdbquery);

SELECT pg_typeof('beer'::zdbquery::text);
SELECT pg_typeof('{"term":"beer"}'::zdbquery::json);
SELECT pg_typeof('{"term":"beer"}'::zdbquery::jsonb);
