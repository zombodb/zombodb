#! /bin/bash

echo -n "Where is PG config:  "
which pg_config

echo "PATH=$PATH"
echo
echo

ls -la /usr/lib/postgresql/

echo "pg_config output"
pg_config

# make clean install

# always return cleanly
exit 0;
