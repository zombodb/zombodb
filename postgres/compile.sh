#! /bin/bash

set -x

# show the environment that we're compiling against
pg_config

# compile the extension
make clean
make

WHOAMI=$(id -u)
if [ `uname` = "Darwin" ] ; then
	PG_OWNER=$(stat -f %u `pg_config --libdir`)
else
	PG_OWNER=$(stat --format %u `pg_config --libdir`)
fi

if [ "$WHOAMI" = "$PG_OWNER" ] ; then
	make install
else
	sudo make install
fi

