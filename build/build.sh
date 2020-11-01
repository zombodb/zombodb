#! /bin/bash

# set -x

LOGDIR=`pwd`/target/logs
REPODIR=`pwd`/target/zombodb
BRANCH=$1
IMAGES=$2
PGVERS=$3

mkdir -p $LOGDIR > /dev/null

if [ "x${BRANCH}" == "x" ] ; then
	echo "usage:  ./build.sh <branch_name>"
	exit 1
fi

if [ "x${IMAGES}" == "x" ] ; then
	IMAGES=$(ls -d zombodb-build-*)
fi

if [ "x${PGVERS}" == "x" ] ; then
	PGVERS="pg10 pg11 pg12"
fi

function build_zdb {
	image=$1
	BUILDDIR=$2
	LOGDIR=$3
	REPODIR=$4
	PGVER=$5

	echo "${image}-${PGVER}:  Building docker image..."
        docker build -t $1 $1 > ${LOGDIR}/${image}-${PGVER}-build.log 2>&1 || exit $?

	echo "${image}-${PGVER}:  Copying ZomboDB code"
	rm -rf ${BUILDDIR} > /dev/null
	mkdir ${BUILDDIR}
	cp -Rp ${REPODIR} ${BUILDDIR}

	echo "${image}-${PGVER}:  Updating cargo-pgx"
	docker run -t ${image} cargo install cargo-pgx > ${LOGDIR}/${image}-${PGVER}-cargo-install-cargo-pgx.log 2>&1 || exit $? 

	echo "${image}-${PGVER}:  Building ZomboDB"
	docker run \
		-e pgver=${PGVER} \
		-w /build/zombodb \
		-v ${BUILDDIR}:/build \
		-t ${image} \
		bash -c \
			'PATH=$(dirname $(cat ~/.pgx/config.toml | grep $pgver | cut -f2 -d= | cut -f2 -d\")):$PATH; \
			PGX_BUILD_VERBOSE=true;\
			cargo pgx package' > ${LOGDIR}/${image}-${PGVER}-cargo-pgx-package.log 2>&1 || exit $?

	echo "${image}-${PGVER}:  finished"
}

export -f build_zdb

echo "Cloning ZomboDB's ${BRANCH} branch"
rm -rf ${REPODIR} > /dev/null
git clone \
	--depth 1 \
	--single-branch \
	--branch ${BRANCH} \
	https://github.com/zombodb/zombodb.git \
	${REPODIR} > ${LOGDIR}/git-clone.log 2>&1 || exit $? 

for image in ${IMAGES}; do
	for pgver in ${PGVERS}; do
		BUILDDIR=`pwd`/target/build/${image}-${pgver}

        	mkdir -p ${BUILDDIR} > /dev/null || exit 1
		printf "%s\0%s\0%s\0%s\0%s\0" "${image}" "${BUILDDIR}" "${LOGDIR}" "${REPODIR}" "${pgver}"
	done
done | xargs -0 -n 5 -P 64 bash -c 'build_zdb "$@"' --
