#
# Copyright 2018-2020 ZomboDB, LLC
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
FROM centos:6

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

RUN yum clean all

##
## to get a modern verison of curl on centos6 we add this "CityFan" repo
##
RUN echo -e '\
[CityFan]\n\
name=City Fan Repo\n\
baseurl=http://www.city-fan.org/ftp/contrib/yum-repo/rhel$releasever/$basearch/\n\
enabled=1\n\
gpgcheck=0\n\
' > /etc/yum.repos.d/city-fan.repo

## and directly install what'll be one of curl's dependencies
RUN rpm -Uvh http://dl.fedoraproject.org/pub/epel/6/x86_64/Packages/l/libnghttp2-1.6.0-1.el6.1.x86_64.rpm

RUN yum update -y
RUN yum install -y tar
RUN yum install -y openssl
RUN yum install -y https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-6-x86_64/pgdg-centos10-10-2.noarch.rpm
RUN yum install -y postgresql10-devel
RUN yum install -y gcc make zlib-devel libnghttp2 curl-devel

ENV PATH /usr/pgsql-10/bin:$PATH


