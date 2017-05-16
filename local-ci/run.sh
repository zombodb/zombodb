#! /bin/bash
#
# Copyright 2015-2017 ZomboDB, LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

DOCKER_IMAGE=zdb-pg95-local-ci

echo "Archiving working tree"
tar czf /tmp/work-tree.tgz --exclude ".git/**/*" ../
mv /tmp/work-tree.tgz ./config/work-tree.tgz

echo "Building docker image"
docker build --build-arg uid=`id -u` --build-arg user=`whoami` -t $DOCKER_IMAGE . || exit 1

echo "Running tests"
docker run --rm -m 6G --oom-kill-disable=true -w /build/zombodb $DOCKER_IMAGE

rm ./config/work-tree.tgz