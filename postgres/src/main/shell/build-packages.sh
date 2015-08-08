#! /bin/bash

VERSION=$1
BASE=`pwd`
DISTROS="centos6 centos7 ubuntu_trusty ubuntu_precise"
POSTGRES_VERSION="9.3"

##
# compile ZomboDB for target distros
##
for distro in ${DISTROS} ; do
    cd $BASE

    mkdir -p $BASE/target/${distro}

    cd src/main/docker/zombodb-build-${distro}

    docker build -t zombodb-build-${distro} .
    docker run --rm -v $BASE:/mnt -w /mnt -e DESTDIR=target/${distro} zombodb-build-${distro} make clean install

    # move the zombod.so into the plugins/ directory
    cd $BASE/target
    cd `dirname $(find ${distro} -name "zombodb.so")`
    mkdir plugins
    cd plugins
    mv ../zombodb.so .
done


##
# also build a tarball from the Ubuntu_precise version
##

cd $BASE
cd target
rm -rf tarball
mkdir -p tarball/lib tarball/share
cp -Rp ubuntu_precise/usr/lib/postgresql/${POSTGRES_VERSION}/lib/* tarball/lib
cp -Rp ubuntu_precise/usr/share/postgresql/${POSTGRES_VERSION}/* tarball/share
cd tarball/
tar czf ../zombodb-$VERSION.tgz .
