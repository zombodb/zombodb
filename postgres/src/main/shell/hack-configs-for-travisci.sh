#! /bin/sh

echo << DONE >> /etc/elasticsearch/elasticsearch.yml
script.disable_dynamic: false
threadpool.bulk.queue_size: 1024
threadpool.bulk.size: 12
http.max_content_length: 1024mb
index.query.bool.max_clause_count: 1000000
DONE

echo "local_preload_libraries='zombodb'" >> /etc/postgresql/postgresql.conf