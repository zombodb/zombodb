#! /bin/sh

cat << DONE >> /etc/elasticsearch/elasticsearch.yml
script.disable_dynamic: false
threadpool.bulk.queue_size: 10
threadpool.bulk.size: 2
http.max_content_length: 1024mb
index.query.bool.max_clause_count: 1000000
DONE

cat << DONE >> /etc/postgresql/9.3/main/postgresql.conf
local_preload_libraries='zombodb.so'
client_min_messages=notice
autovacuum=off
DONE
