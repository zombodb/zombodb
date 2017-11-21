#! /bin/bash

cat << DONE >> /etc/elasticsearch/elasticsearch.yml
script.disable_dynamic: false
threadpool.bulk.queue_size: 1024
threadpool.bulk.size: 12
http.max_content_length: 1024mb
index.query.bool.max_clause_count: 1000000
DONE

cat << DONE >> /etc/postgresql/9.5/main/postgresql.conf
local_preload_libraries='zombodb.so'
client_min_messages=notice
autovacuum=off
max_connections=10
work_mem=64kB
fsync=off
zombodb.default_elasticsearch_url='http://localhost:9200/'
DONE

cat << DONE > /etc/postgresql/9.5/main/pg_hba.conf
local   all             all                                     trust
DONE
