CREATE TYPE zdb_termlist_response AS (
  term text,
  totalfreq bigint,
  docfreq bigint
);
CREATE OR REPLACE FUNCTION zdb_internal_termlist(indexrel oid, fieldname text, prefix text, startat text, size int4) RETURNS json IMMUTABLE LANGUAGE c AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_termlist(table_name regclass, fieldname text, prefix text, startat text, size int4) RETURNS SETOF zdb_termlist_response IMMUTABLE LANGUAGE plpgsql AS $$
DECLARE
  indexRel oid;
  response json;
BEGIN
  indexRel := zdb_determine_index(table_name);
  response := zdb_internal_termlist(indexRel, fieldname, prefix, startat::TEXT, size);

  RETURN QUERY SELECT
                 (value->>'term') AS term,
                 sum((value->>'totalfreq')::bigint)::bigint AS totalfreq,
                 sum((value->>'docfreq')::bigint)::bigint AS docfreq
               FROM json_array_elements(response->'terms') value
               GROUP BY 1
               ORDER BY 1
               LIMIT CASE WHEN size > 0 THEN size ELSE 2147483647 END;
END;
$$;