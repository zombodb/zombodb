#! /bin/bash

mkdir -p target/artifacts
for f in $(find target -name "*.deb") $(find target -name "*.rpm"); do
	cp $f target/artifacts
done

[[ $(uname) == "Darwin" ]] && open target/artifacts
