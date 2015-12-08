#! /bin/sh

curl https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.7.0.tar.gz > es170.tgz
tar xzf ./es170.tgz
ln -s elasticsearch-1.7.0 ./es

cat << DONE >> ./es/elasticsearch.yml
script.disable_dynamic: false
threadpool.bulk.queue_size: 10
threadpool.bulk.size: 2
http.max_content_length: 1024mb
index.query.bool.max_clause_count: 1000000
DONE

cat << DONE >> /var/lib/pgsql/9.3/data/postgresql.conf
local_preload_libraries='zombodb.so'
client_min_messages=notice
autovacuum=off
max_connections=10
work_mem=64kB
DONE
