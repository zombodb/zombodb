#! /bin/bash

PGVER=$1
DEBRPM=$2

if [ "x${PGVER}" == "x" ] || [ "x${DEBRPM}" == "x" ]; then
	echo 'usage:  ./package.sh <PGVER> <deb or rpm>'
	exit 1
fi


PG_CONFIG_DIR=$(dirname $(grep ${PGVER} ~/.pgx/config.toml | cut -f2 -d= | cut -f2 -d\"))
export PATH=${PG_CONFIG_DIR}:${PATH}

cargo install cargo-pgx
cargo install cargo-deb
cargo pgx package || exit $?

if [ "${DEBRPM}" == "deb" ]; then
	cargo deb --no-build --no-strip --variant ${PGVER} || exit $?
elif [ "${DEBRPM}" == "rpm" ]; then
	echo buiding rpm...
else
	echo Unrecognized value for DEBRPM:  ${DEBRPM}
	exit 1
fi

