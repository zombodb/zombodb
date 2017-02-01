-- USAGE: $ psql -d postgres -U postgres -f sql/teardown.sql

--Kill any lingering open connections to the dB
SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'zdbtaptests';

DROP DATABASE IF EXISTS zdbtaptests;

\! curl -s -XDELETE 'http://127.0.0.1:9200/zdbtaptests.*' > /dev/null
