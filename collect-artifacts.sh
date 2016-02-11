#! /bin/sh

mkdir -p target/artifacts
cp $(find ./ -name "zombodb*.deb") $(find ./ -name "zombodb*.rpm") $(find ./ -name "zombodb*.tgz") $(find ./ -name "zombodb*.zip") target/artifacts
