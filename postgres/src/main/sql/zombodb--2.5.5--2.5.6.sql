CREATE TYPE zdb_get_index_field_lists_response AS (fieldname text, fields text[]);
CREATE OR REPLACE FUNCTION zdb_internal_get_index_field_lists(index_oid oid) RETURNS text LANGUAGE c STRICT IMMUTABLE AS '$libdir/plugins/zombodb';
CREATE OR REPLACE FUNCTION zdb_get_index_field_lists(table_name regclass) RETURNS SETOF zdb_get_index_field_lists_response LANGUAGE sql STRICT IMMUTABLE AS $$
    select trim((regexp_matches(x, '(.*)='))[1]) as fieldname, string_to_array((regexp_matches(x, '=\s*\[(.*?)(\]|$)'))[1], ',') fields
      from regexp_split_to_table(zdb_internal_get_index_field_lists(zdb_determine_index($1)), '\]\s*,') x;
$$;
