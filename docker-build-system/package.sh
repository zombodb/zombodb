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


PGVER=$1
IMAGE=$2

if [ "x${PGVER}" == "x" ] || [ "x${IMAGE}" == "x" ] ; then
	echo 'usage:  ./package.sh <PGVER> <image>'
	exit 1
fi

if [[ ${IMAGE} == *"fedora"* ]] || [[ ${IMAGE} == *"centos"* ]]; then
	PKG_FORMAT=rpm
elif [[ ${IMAGE} == *"alpine"* ]]; then
	PKG_FORMAT=apk
else
	PKG_FORMAT=deb
fi

set -x

OSNAME=$(echo ${IMAGE} | cut -f3-4 -d-)
VERSION=$(cat zombodb.control | grep default_version | cut -f2 -d\')


PG_CONFIG_DIR=$(dirname $(grep ${PGVER} ~/.pgx/config.toml | cut -f2 -d= | cut -f2 -d\"))
export PATH=${PG_CONFIG_DIR}:${PATH}

#
# ensure cargo-pgx is up-to-date
#
cargo install cargo-pgx

#
# build the extension
#
cargo pgx package || exit $?

#
# cd into the package directory
#
BUILDDIR=`pwd`
cd target/release/zombodb-${PGVER} || exit $?

# strip the binaries to make them smaller
find ./ -name "*.so" -exec strip {} \;

#
# then use 'fpm' to build either a .deb, .rpm or .apk
#
if [ "${PKG_FORMAT}" == "deb" ]; then
	fpm \
		-s dir \
		-t deb \
		-n zombodb-${PGVER} \
		-v ${VERSION} \
		--deb-no-default-config-files \
		-p ${BUILDDIR}/target/release/zombodb_${OSNAME}_${PGVER}-${VERSION}_amd64.deb \
		-a amd64 \
		. || exit 1

elif [ "${PKG_FORMAT}" == "rpm" ]; then
	fpm \
		-s dir \
		-t rpm \
		-n zombodb-${PGVER} \
		-v ${VERSION} \
		--rpm-os linux \
		-p ${BUILDDIR}/target/release/zombodb_${OSNAME}_${PGVER}-${VERSION}_1.x86_64.rpm \
		-a x86_64 \
		. || exit 1

elif [ "${PKG_FORMAT}" == "apk" ]; then
	fpm \
		-s dir \
		-t apk \
		-n zombodb-${PGVER} \
		-v ${VERSION} \
		-p ${BUILDDIR}/target/release/zombodb_${OSNAME}_${PGVER}-${VERSION}.$(uname -m).apk \
		-a $(uname -m) \
		. \
		|| exit 1

else
	echo Unrecognized value for PKG_FORMAT:  ${PKG_FORMAT}
	exit 1
fi

