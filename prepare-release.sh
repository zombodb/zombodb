#! /bin/bash

if [ "x$2" == "x" ] ; then
	echo "Usage:  ./prepare-release.sh old-version new-version"
	exit 1
fi

cargo install --git https://github.com/zombodb/pg-schema-diff.git
cargo pgx dump-schema ./sql/releases
echo "diffing schema..."
pg-schema-diff diff sql/releases/zombodb--$1.sql sql/releases/zombodb--$2.sql > sql/zombodb--$1--$2.sql
