#! /bin/bash


cat << DONE >> /etc/postgresql/10/main/postgresql.conf
client_min_messages=notice
autovacuum=off
fsync=off
zdb.default_elasticsearch_url = 'http://localhost:9200/'
zdb.log_level = LOG
zdb.default_replicas = 0
DONE

