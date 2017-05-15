#! /bin/bash

VERSION=$1
BASE=`pwd`
DISTROS="centos6 centos7 ubuntu_trusty ubuntu_precise debian_jessie"
POSTGRES_VERSIONS="9.3 9.4 9.5"

echo "Archiving working directory tree"
tar cf /tmp/zdb-build.tgz .

##
# compile ZomboDB for target distros
##
for POSTGRES_VERSION in ${POSTGRES_VERSIONS} ; do
    for distro in ${DISTROS} ; do
        cd $BASE

        mkdir -p $BASE/target/pg${POSTGRES_VERSION}/${distro}

        DOCKER_IMAGE=zombodb-build-${POSTGRES_VERSION}-${distro}
        cd src/main/docker/pg${POSTGRES_VERSION}/zombodb-build-${distro}

        echo "BUILDING: $distro, $POSTGRES_VERSION ****"
        docker build --build-arg user=`whoami` --build-arg uid=`id -u` -t $DOCKER_IMAGE . > $BASE/target/pg${POSTGRES_VERSION}/${distro}/docker-build.log

        echo "   making /tmp/zdb-build"
        docker run -w /tmp/ $DOCKER_IMAGE mkdir -p /tmp/zdb-build/
        CONTAINER_ID=$(docker ps -l | grep $DOCKER_IMAGE | awk '{print $1}')
        docker commit $CONTAINER_ID $DOCKER_IMAGE-inflight &> /dev/null

        echo "   copying archive"
        cat /tmp/zdb-build.tgz | docker run -i -w /tmp/zdb-build/ $DOCKER_IMAGE-inflight tar xf -
        CONTAINER_ID=$(docker ps -l | grep $DOCKER_IMAGE-inflight | awk '{print $1}')
        docker commit $CONTAINER_ID $DOCKER_IMAGE-inflight &> /dev/null

        echo "   compiling zombodb"
        (docker run -w /tmp/zdb-build -e DESTDIR=/tmp/target/pg${POSTGRES_VERSION}/${distro} $DOCKER_IMAGE-inflight \
            make clean install &> $BASE/target/pg${POSTGRES_VERSION}/${distro}/compile.log) || exit 1
        CONTAINER_ID=$(docker ps -l | grep $DOCKER_IMAGE-inflight | awk '{print $1}')
        docker commit $CONTAINER_ID $DOCKER_IMAGE-inflight &> /dev/null

        echo "   saving artifacts"
        cd $BASE/target
        docker run --rm -w /tmp/target $DOCKER_IMAGE-inflight tar cf - . | tar xf -

        # move the zombod.so into the plugins/ directory
        cd $BASE/target/pg${POSTGRES_VERSION}
        cd `dirname $(find ${distro} -name "zombodb.so")`
        mkdir plugins
        cd plugins
        mv ../zombodb.so .

        ##
        # also build a tarball from the Ubuntu_precise version
        ##
        if [ $distro == "ubuntu_precise" ] ; then
            cd $BASE
            cd target/pg${POSTGRES_VERSION}
            rm -rf tarball
            mkdir -p tarball/lib tarball/share
            cp -Rp ubuntu_precise/usr/lib/postgresql/${POSTGRES_VERSION}/lib/* tarball/lib
            cp -Rp ubuntu_precise/usr/share/postgresql/${POSTGRES_VERSION}/* tarball/share
            cd tarball/
            tar czf ../zombodb-precise-pg${POSTGRES_VERSION}-${VERSION}.tgz .
        fi
    done

done

echo "Removing all inflight docker images"
docker rmi -f $(docker images | grep "inflight" | awk '{print $3}')