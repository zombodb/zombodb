CREATE OR REPLACE FUNCTION rest_post(url text, postdata text) RETURNS json AS '$libdir/plugins/zombodb' language c;
CREATE TYPE xact AS (ctid tid, xmin xid, xmax xid, cmin cid, cmax cid);
CREATE OR REPLACE FUNCTION zdb_tid_to_zdb_id(ctid tid) RETURNS text LANGUAGE sql AS $$
  SELECT substring(id, 1, length(id) - 1)
    FROM (SELECT format('%s-%s', substring(ctid :: TEXT, 2, strpos(ctid :: TEXT, ',') - 2),
                    substring(ctid :: TEXT, strpos(ctid :: TEXT, ',') + 1)) id) x;
$$;
CREATE OR REPLACE FUNCTION zdb_index_repair(table_name REGCLASS)
  RETURNS INT8 LANGUAGE plpgsql AS
$$
DECLARE
  repaired      INT8 := 0;
  pkey          text := zdb_get_index_mapping(table_name)->'mappings'->'xact'->'_meta'->>'primary_key';
  indexOid      OID := zdb_determine_index(table_name);
  es_index_name TEXT := zdb_get_index_name(indexOid :: REGCLASS);
  url           TEXT := zdb_get_url(indexOid::regclass);
  pages         int;
  i             int := 0;
  pct           NUMERIC;
  r             RECORD;
  xelem         xact;
  q             text;
  results       json;
  total         int;
  hits          json;
  missing       record;
BEGIN
  EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', table_name);
  RAISE NOTICE '% locked in exclusive mode', table_name;
  RAISE NOTICE 'Querying xact data from Postgres table %', table_name;
  EXECUTE format('SELECT relpages FROM pg_class WHERE oid = ''%s''::regclass', table_name) INTO pages;
  FOR r IN EXECUTE format($s$
            SELECT array_agg(ROW(ctid, xmin, xmax, cmin, cmax)::xact) xact, substring(ctid::text, 2, strpos(ctid::text, ',') - 2) pageno FROM %s GROUP BY 2
        $s$, table_name) LOOP

    q := ''; -- used in query string, so spaces are encoded as plus signs
    FOREACH xelem IN ARRAY r.xact LOOP
      IF length(q) > 0 THEN q := format('%s+or', q); END IF;
      q := format('%s+_id:%s', q, zdb_tid_to_zdb_id(xelem.ctid));
    END LOOP;

    pct := round(i/pages::NUMERIC*100, 4);
    IF i % 1000 = 0 THEN
      RAISE NOTICE 'Querying ES for page #% (% of %): %%% complete', r.pageno, i, pages, pct;
    END IF;
    results := rest_get(format('%s%s/xact/_search?q=%s', url, es_index_name, q));

    total := (results->'hits'->>'total')::int;
    hits := results->'hits'->'hits';
    IF total <> array_upper(r.xact, 1) THEN
      -- missing at least one xact record, so figure out which one
      -- and then add it back to Elasticsearch
      FOR missing IN SELECT (unnest(r.xact)).ctid AS ctid EXCEPT SELECT zdb_id_to_ctid((json_array_elements(hits)->>'_id')::text) LOOP
        RAISE NOTICE '   REPAIRING: %...', missing.ctid;
        PERFORM rest_post(format('%s%s/xact/%s?refresh=true', url, es_index_name, zdb_tid_to_zdb_id(missing.ctid)), '{}');
        raise notice '   ...repaired';

        raise notice '   UPDATING %...', missing.ctid;
        EXECUTE format('UPDATE %s SET %s = %s WHERE ctid=''%s''', table_name, pkey, pkey, missing.ctid);
        raise notice '   ...updated';

        repaired := repaired + 1;
      END LOOP;
    END IF;

    i := i+1;
  END LOOP;

  RETURN repaired;
END;
$$;
