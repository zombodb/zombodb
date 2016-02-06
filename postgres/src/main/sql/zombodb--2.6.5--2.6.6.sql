DO LANGUAGE plpgsql $$
BEGIN
  PERFORM name FROM zdb_analyzers WHERE name = 'default';
  IF NOT FOUND THEN
    INSERT INTO zdb_analyzers(name, definition, is_default) VALUES (
      'default', '{
          "tokenizer": "keyword",
          "filter": ["trim", "zdb_truncate_32000", "lowercase"]
        }', true);
    RAISE NOTICE 'Added a ''default'' analyzer.  Any tables with fields of type ''json'' will need to be REINDEX''d';
  END IF;
END;
$$;