SELECT zdb_update_mapping('so_posts');

/* now make sure the things that are important haven't changed */
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_meta'->>'primary_key' = 'id';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_all'->>'analyzer' = 'phrase';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_field_names'->>'type' = '_field_names';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_field_names'->>'index' = 'no';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_source'->>'enabled' = 'false';
