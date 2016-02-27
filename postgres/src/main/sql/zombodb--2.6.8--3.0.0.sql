DROP OPERATOR ==>(json, text);

CREATE OPERATOR CLASS zombodb_jsonb_ops DEFAULT FOR TYPE jsonb USING zombodb AS STORAGE jsonb;
CREATE OR REPLACE FUNCTION zdb_to_jsonb(anyelement) RETURNS jsonb LANGUAGE internal IMMUTABLE STRICT AS $$to_jsonb$$;

DROP FUNCTION zdbgetbitmap(internal, internal);
UPDATE pg_am SET amgetbitmap = '-' WHERE amname = 'zombodb';