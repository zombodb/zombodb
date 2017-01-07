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

export VERSION=$(grep default_version postgres/zombodb.control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\\([^']*\\)'/\\1/")

mvn clean install

/usr/share/elasticsearch/bin/plugin -i zombodb -u file:///build/zombodb/elasticsearch/target/zombodb-es-plugin-${VERSION}.zip
/etc/init.d/elasticsearch start

mvn clean

cd postgres
make clean install
mkdir /usr/lib/postgresql/9.5/lib/plugins
cp /usr/lib/postgresql/9.5/lib/zombodb.so /usr/lib/postgresql/9.5/lib/plugins/zombodb.so
rm /usr/lib/postgresql/9.5/lib/zombodb.so
src/main/shell/hack-configs-for-travisci.sh
make clean
sudo /etc/init.d/postgresql start 9.5
createuser -s -U postgres root

sleep 5
make installcheck
sudo chown -R $HOST_USER:$HOST_USER regression* results > /dev/null 2>&1
