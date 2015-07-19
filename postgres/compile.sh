#! /bin/bash

# show the environment that we're compiling against
pg_config

# compile the extension
make clean install

# always exit cleanly
exit 0;
