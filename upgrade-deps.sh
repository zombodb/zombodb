#! /bin/bash

# requires:  "cargo install cargo-edit" from https://github.com/killercup/cargo-edit

DIRS=". docker-build-system"

for d in $DIRS; do
	cd ${d}
	cargo update
	cargo upgrade --pinned
	cargo generate-lockfile
done

