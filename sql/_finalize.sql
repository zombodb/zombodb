CREATE OR REPLACE FUNCTION zdb.get_search_analyzer(index regclass, field text) RETURNS text
    IMMUTABLE STRICT PARALLEL SAFE
    LANGUAGE sql AS
$$
WITH properties AS (
    SELECT zdb.index_mapping(index) -> zdb.index_name(index) -> 'mappings' -> 'properties' ->
           field AS props)
SELECT COALESCE(props ->> 'search_analyzer', props ->> 'analyzer', 'standard')
FROM properties
LIMIT 1;

$$;
