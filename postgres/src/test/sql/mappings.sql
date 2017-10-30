----------
-- DATA --
----------

-- _meta --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_meta'->>'primary_key' = 'id';

-- _all --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_all'->>'analyzer' = 'phrase';

-- _source --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_source'->>'enabled' = 'false';
