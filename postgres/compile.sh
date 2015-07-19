#! /bin/bash

# show the environment that we're compiling against
pg_config

# compile the extension
make clean
make

WHOAMI=$(id -u)
PG_OWNER=$(stat -f %u `pg_config --libdir`)
if [ $WHOAMI = $PG_OWNER ] ; then
	make install
else
	sudo make insall
fi

