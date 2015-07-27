CREATE OR REPLACE FUNCTION assert(actual int8, expected int8, message text) RETURNS bool LANGUAGE plpgsql AS $$
BEGIN
  IF expected IS DISTINCT FROM actual THEN
    RAISE EXCEPTION 'ASSERT: %: expected=%, actual=%', message, expected, actual;
  END IF;

  RETURN true;
END;
$$;
