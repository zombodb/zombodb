#! /bin/bash

echo -n "Where is PG config:  "
which pg_config
ls -la `which pg_config`

echo "PATH=$PATH"
echo
echo

echo "/usr/lib/postgresql/"
ls -la /usr/lib/postgresql/
echo

echo "/usr/lib/postgresql/9.3/bin"
ls -la /usr/lib/postgresql/9.3/bin


export PATH=/usr/lib/postgresql/9.3/bin:$PATH
echo "pg_config output"
pg_config


make clean install

# always return cleanly
exit 0;
