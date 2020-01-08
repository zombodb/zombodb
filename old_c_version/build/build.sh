#
# Copyright 2018-2019 ZomboDB, LLC
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
#! /bin/bash

BUILDDIR=`pwd`
BASE=$(dirname `pwd`)
VERSION=$(cat $BASE/zombodb.control | grep default_version | cut -f2 -d\')
LOGDIR=${BASE}/target/logs
ARTIFACTDIR=${BASE}/target/artifacts

mkdir -p ${LOGDIR}
mkdir -p ${ARTIFACTDIR}

rm -rf ${ARTIFACTDIR}
for image in `ls docker/` ; do
    OS_DIST=$(echo ${image}|cut -f3 -d-)
    OS_VER=$(echo ${image}|cut -f4 -d-)
    PG_VER=$(echo ${image}|cut -f5 -d-)
    cd ${BUILDDIR}

    cd docker/${image}
    echo ${image}
    echo "     Building Docker image"
    docker build -t ${image} . 2>&1 > ${LOGDIR}/${image}-build.log || exit 1

    echo "     Compiling ZomboDB"
    docker run -e DESTDIR=/build/target/artifacts/${image} -w /build -v ${BASE}:/build -t ${image} make clean install 2>&1 > ${LOGDIR}/${image}-compile.log || exit 1

    echo "     Cleaning"
    docker run -w /build -v ${BASE}:/build -t ${image} make clean 2>&1 > ${LOGDIR}/${image}-clean.log || exit 1

    cd ${ARTIFACTDIR}/${image}
    echo ${image} | grep centos 2>&1 > /dev/null
    if [ "$?" == "0" ] ; then
        echo "     building rpm package"
        fpm -s dir -t rpm -n zombodb -v ${VERSION} --rpm-os linux -p ${ARTIFACTDIR}/zombodb_${OS_DIST}${OS_VER}_${PG_VER}-${VERSION}_1.x86_64.rpm -a x86_64 . || exit 1
    else
        echo "     building deb package"
        fpm --deb-no-default-config-files -s dir -t deb -n zombodb -v ${VERSION} -p ${ARTIFACTDIR}/zombodb_${OS_VER}_${PG_VER}-${VERSION}_amd64.deb -a amd64 . || exit 1
    fi
done
