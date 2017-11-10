#! /bin/bash

cat << DONE >> ~/elasticsearch-5.6.4/config/elasticsearch.yml
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
DONE

cat << DONE > /etc/postgresql/9.5/main/pg_hba.conf
local   all             all                                     trust
DONE
