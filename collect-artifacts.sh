#! /bin/sh

mkdir -p target/artifacts
cp $(find ./ -name "zombodb*.deb") $(find ./ -name "zombodb*.rpm") $(find ./ -name "zombodb*.tgz") $(find ./ -name "zombodb*.zip") target/artifacts
ls -la target/artifacts

if [ `uname` == 'Linux' ] ; then
	xdg-open target/artifacts
else
	open target/artifacts
fi
