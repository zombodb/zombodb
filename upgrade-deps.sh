#! /bin/bash

# requires:  "cargo install cargo-edit" from https://github.com/killercup/cargo-edit

cargo update
cargo upgrade
cargo generate-lockfile

