CREATE OR REPLACE FUNCTION test(table_name regclass, ctid tid)
  RETURNS TID LANGUAGE C
IMMUTABLE
STRICT AS '$libdir/plugins/zombodb', 'zdb_table_ref_and_tid';

CREATE INDEX idxtest
  ON so_posts
  USING zombodb (
  test('so_posts':: REGCLASS, ctid),
  zdb(so_posts.*)) WITH (shadow='idxso_posts',
OPTIONS =
  'owner_user_id=<so_users.idxso_users>does_not_exist,
   last_editor_user_id=<so_comments.idxso_comments>user_id,
   owner_user_id=<products.idx_zdb_products>id
', always_resolve_joins='true'
);

CREATE VIEW test AS
  SELECT
    *,
    test('so_posts', ctid) AS zdb
  FROM so_posts;


DO LANGUAGE plpgsql $$
DECLARE
  err text;
BEGIN
  BEGIN
    SELECT count(*) FROM test WHERE zdb ==> '#expand<inventory_count=<this.index>inventory_count>(beer)';
  EXCEPTION WHEN others THEN
    GET STACKED DIAGNOSTICS err = MESSAGE_TEXT;
    IF err ILIKE '%does_not_exist does not exist in contrib_regression.public.so_users.idxso_users%' THEN
      RAISE NOTICE 'found correct error message from Elasticsearch';
    ELSE
      RAISE EXCEPTION '%', err;
    END IF;
  END;
END;
$$;

DROP INDEX idxtest;
CREATE INDEX idxtest
  ON so_posts
  USING zombodb (
  test('so_posts':: REGCLASS, ctid),
  zdb(so_posts.*)) WITH (shadow='idxso_posts',
OPTIONS =
  'owner_user_id=<so_users.idxso_users>id,
   last_editor_user_id=<so_comments.idxso_comments>user_id,
   owner_user_id=<products.idx_zdb_products>id
', always_resolve_joins='true'
);

SELECT count(*) FROM test WHERE zdb ==> '#expand<inventory_count=<this.index>inventory_count>(beer)';


DROP INDEX idxtest;
DROP VIEW test;
DROP FUNCTION test(regclass, tid);
