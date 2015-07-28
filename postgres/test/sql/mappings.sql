----------
-- XACT --
----------

-- _meta --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'_meta'->>'primary_key' = 'id';

-- _all --
SELECT (zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'_all'->>'enabled')::boolean = false;

-- _field_names --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'_field_names'->>'type' = '_field_names';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'_field_names'->>'index' = 'no';

-- properties --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_cmax'->>'type' = 'integer';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_cmax'->'fielddata'->>'format' = 'disabled';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_cmin'->>'type' = 'integer';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_cmin'->'fielddata'->>'format' = 'disabled';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_partial'->>'type' = 'boolean';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_partial'->'fielddata'->>'format' = 'disabled';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_xmax'->>'type' = 'integer';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_xmax'->'fielddata'->>'format' = 'disabled';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_xmax_is_committed'->>'type' = 'boolean';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_xmax_is_committed'->'fielddata'->>'format' = 'disabled';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_xmin'->>'type' = 'integer';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_xmin'->'fielddata'->>'format' = 'disabled';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_xmin_is_committed'->>'type' = 'boolean';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'xact'->'properties'->'_xmin_is_committed'->'fielddata'->>'format' = 'disabled';

----------
-- DATA --
----------

-- _meta --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_meta'->>'primary_key' = 'id';

-- _all --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_all'->>'analyzer' = 'phrase';

-- _parent --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_parent'->>'type' = 'xact';

-- _routing --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_routing'->>'required' = 'true';

-- _field_names --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_field_names'->>'type' = '_field_names';
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_field_names'->>'index' = 'no';

-- _source --
SELECT zdb_get_index_mapping('so_posts')->'mappings'->'data'->'_source'->>'enabled' = 'false';
