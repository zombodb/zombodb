#! /bin/bash

# show the environment that we're compiling against
set -x
pg_config

# compile the extension
make clean
make
