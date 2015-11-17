#! /bin/sh

OLD=$1
NEW=$2

if [ "${OLD}" == "" ] || [ "${NEW}" == "" ] ; then
	echo Usage: ./update-versions.sh OLDVER NEWVER
	exit 1
fi
for f in `find . -name pom.xml` postgres/zombodb.control docker/postgres/Dockerfile docker/elasticsearch/Dockerfile .gitignore ; do
	echo Processing: $f
	sed -i.bak s/${OLD}/${NEW}/g $f
done

SQL=postgres/src/main/sql/zombodb--${OLD}--${NEW}.sql
echo "-- no sql changes" > ${SQL}
git add ${SQL}
