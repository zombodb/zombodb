SELECT v->'top-tags'->'buckets'
FROM zdb_json_aggregate('so_posts', '{ "top-tags":{"terms":{"field":"tags", "size" : 3}}}', 'java') v;