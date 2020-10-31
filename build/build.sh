#! /bin/bash

# set -x

LOGDIR=target/logs

mkdir -p $LOGDIR
for image in `ls -d zombodb-build-*` ; do
	echo $image 

	echo "   Building..."
	docker build -t $image $image 2>&1 > ${LOGDIR}/$image-build.log || exit $?

done
