create or replace function explain(query text) RETURNS json LANGUAGE plpgsql AS
$$
DECLARE
  result json;
BEGIN
  EXECUTE ('EXPLAIN (format json)' || query) INTO result;

  RETURN result;
END;
$$;


SELECT assert(
    (json_array_element(explain('select id from so_posts where zdb(''so_posts'', ctid) ==> '''''), 0)->'Plan'->'Plan Rows')::text::int8,
    2500,
    'built-in default');

alter index idxso_posts set (default_row_estimate = 5000);
SELECT assert(
    (json_array_element(explain('select id from so_posts where zdb(''so_posts'', ctid) ==> '''''), 0)->'Plan'->'Plan Rows')::text::int8,
    5000,
    'explicit estimate of 5k');

alter index idxso_posts set (default_row_estimate = -1);
SELECT assert(
    (json_array_element(explain('select id from so_posts where zdb(''so_posts'', ctid) ==> '''''), 0)->'Plan'->'Plan Rows')::text::int8,
    165240,
    'actual row count');

-- restore back to default of 2500
alter index idxso_posts reset (default_row_estimate);
-- and force us to ask the index anyways
SET zombodb.force_row_estimation TO ON;
SELECT assert(
    (json_array_element(explain('select id from so_posts where zdb(''so_posts'', ctid) ==> '''''), 0)->'Plan'->'Plan Rows')::text::int8,
    165240,
    'actual row count, forced');

