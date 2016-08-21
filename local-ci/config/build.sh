#! /bin/bash
#
# Copyright 2015-2016 ZomboDB, LLC
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

export ESV=$(curl -s -XGET 'localhost:9200' | grep number | sed 's/.*: "//;s/\..*//')
export ESV=2
echo "Installing ES ${ESV} plugin..."
if [[ $ESV = "2" ]]
then
    echo "ES2.x"
    /usr/share/elasticsearch/bin/plugin install file:///build/zombodb/elasticsearch/target/zombodb-es-plugin-${VERSION}.zip
else
    /usr/share/elasticsearch/bin/plugin -i zombodb -u file:///build/zombodb/elasticsearch/target/zombodb-es-plugin-${VERSION}.zip
fi
/etc/init.d/elasticsearch start


cd postgres
make clean install
mkdir /usr/lib/postgresql/9.5/lib/plugins
cp /usr/lib/postgresql/9.5/lib/zombodb.so /usr/lib/postgresql/9.5/lib/plugins/zombodb.so
rm /usr/lib/postgresql/9.5/lib/zombodb.so
src/main/shell/hack-configs-for-travisci.sh
sudo /etc/init.d/postgresql start 9.5
createuser -s -U postgres root

sleep 5
make installcheck


# sudo cat /var/log/elasticsearch/*.log
