#! /bin/bash

OLD=$1
NEW=$2

if [ "${OLD}" == "" ] || [ "${NEW}" == "" ] ; then
	echo Usage: ./update-versions.sh OLDVER NEWVER
	exit 1
fi
for f in .gitignore src/c/zombodb.h zombodb.control ; do
	echo Processing: $f
	sed -i.bak s/${OLD}/${NEW}/g $f
done

SQL=src/sql/zombodb--${OLD}--${NEW}.sql
echo "-- no sql changes" > ${SQL}
git add ${SQL}
