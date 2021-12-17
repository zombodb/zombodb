-- USAGE: $ psql -d postgres -U postgres -f sql/teardown.sql

--Kill any lingering open connections to the dB
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'zdbtaptests';

DROP DATABASE IF EXISTS zdbtaptests;

\! (for i in $(curl -s localhost:9200/_cat/aliases | grep zdbtaptests | awk '{ print $2 }') ; do curl -s -XDELETE localhost:9200/$i; done) > /dev/null
