#! /bin/bash
#
# Copyright 2018-2020 ZomboDB, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# set -x

TARGET_DIR=$(pwd)/target
LOGDIR=${TARGET_DIR}/logs
REPODIR=${TARGET_DIR}/zombodb
BRANCH=$1
IMAGES=$2
PGVERS=$3

if [ "x${CPUS}" == "x" ] ; then
  CPUS=64
  echo "Defauling number of CPUs to 64"
fi

mkdir -p $LOGDIR > /dev/null

if [ "x${BRANCH}" == "x" ] ; then
	echo "usage:  ./build.sh <branch_name> [image name] [pgver]"
	exit 1
fi

if [ "x${IMAGES}" == "x" ] ; then
	IMAGES=$(ls -d zombodb-build-*)
fi

if [ "x${PGVERS}" == "x" ] ; then
	PGVERS="pg10 pg11 pg12 pg13"
fi

function exit_with_error {
	echo ERROR:  $1
	exit 1
}

function build_docker_image {
	image=$1
	BUILDDIR=$2
	LOGDIR=$3
	REPODIR=$4
	PGVERS=$5

	for PGVER in ${PGVERS}; do
		echo "${image}-${PGVER}:  Building docker image..."
		docker build \
			--build-arg USER=$USER \
			--build-arg UID=$(id -u) \
			--build-arg GID=$(id -g) \
			--build-arg PGVER="${PGVER#pg}" \
			-t ${image}-${PGVER} \
			${image} \
				> ${LOGDIR}/${image}-${PGVER}-build.log 2>&1 || exit_with_error "${image}-${PGVER}:  image build failed"
	done
}

function build_zdb {
	image=$1
	BUILDDIR=$2
	LOGDIR=$3
	REPODIR=$4
	PGVER=$5

	echo "${image}-${PGVER}:  Copying ZomboDB code"
	rm -rf ${BUILDDIR} > /dev/null
	mkdir ${BUILDDIR}
	cp -Rp ${REPODIR} ${BUILDDIR}

	echo "${image}-${PGVER}:  Building ZomboDB"
	docker run \
		-e pgver=${PGVER} \
		-e image=${image} \
		-w /build/zombodb \
		-v ${BUILDDIR}:/build \
		--rm \
		--user $(id -u):$(id -g) \
		-t ${image}-${PGVER} \
		bash -c './docker-build-system/package.sh $pgver ${image}' \
			> ${LOGDIR}/${image}-${PGVER}-package.sh.log 2>&1 || exit_with_error "${image}-${PGVER}:  build failed"

	echo "${image}-${PGVER}:  finished"
}

export -f build_docker_image
export -f build_zdb
export -f exit_with_error

if [ ! -d "${REPODIR}" ]; then
	echo "Cloning ZomboDB's ${BRANCH} branch"
	rm -rf ${REPODIR} > /dev/null
	mkdir -p $(pwd)/target || exit $?

	git clone \
		--depth 1 \
		--single-branch \
		--branch ${BRANCH} \
		https://github.com/zombodb/zombodb.git \
		${REPODIR} > ${LOGDIR}/git-clone.log 2>&1 || exit $?
fi

for image in ${IMAGES}; do
	printf "%s\0%s\0%s\0%s\0%s\0" "${image}" "${BUILDDIR}" "${LOGDIR}" "${REPODIR}" "${PGVERS}"
done | xargs -0 -n 5 -P ${CPUS} bash -c 'build_docker_image "$@"' --

for image in ${IMAGES}; do
	for pgver in ${PGVERS}; do
		BUILDDIR=${TARGET_DIR}/build/${image}-${pgver}

		printf "%s\0%s\0%s\0%s\0%s\0" "${image}" "${BUILDDIR}" "${LOGDIR}" "${REPODIR}" "${pgver}"
	done
done | xargs -0 -n 5 -P ${CPUS} bash -c 'build_zdb "$@"' --

./collect-artifacts.sh
