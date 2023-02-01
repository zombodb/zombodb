#! /bin/bash

# requires: https://github.com/sunng87/cargo-release

if [ "x$1" == "x" ]; then
	echo "usage:  ./update-verions.sh <VERSION>"
	exit 1
fi

set -x 

HEAD=$(git rev-parse HEAD)
VERSION=$1

cargo release version $1 --execute || exit 1
cd docker-build-system
   cargo release version $1 --execute || exit 1
cd ..

sed -i.bak -e "s/^default_version = .*$/default_version = '${VERSION}'/" zombodb.control || exit 1
rm zombodb.control.bak


