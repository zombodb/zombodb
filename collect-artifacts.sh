#! /bin/bash

mkdir -p target/artifacts
cp $(find ./ -name "zombodb*.deb") $(find ./ -name "zombodb*.rpm") $(find ./ -name "zombodb*.tgz") $(find ./ -name "zombodb*.zip") target/artifacts
zip target/artifacts/artifacts.zip target/artifacts/* &> /dev/null
ls -la target/artifacts

if [ `uname` == 'Linux' ] ; then
	xdg-open target/artifacts > /dev/null 2>&1
else
	open target/artifacts
fi
