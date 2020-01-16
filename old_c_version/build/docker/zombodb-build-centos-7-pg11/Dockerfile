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
FROM centos:7

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

RUN yum update -y
RUN yum install -y tar which wget
RUN yum install -y openssl
RUN yum install -y https://download.postgresql.org/pub/repos/yum/11/redhat/rhel-7-x86_64/pgdg-centos11-11-2.noarch.rpm
RUN yum install -y postgresql11-devel

RUN yum install -y centos-release-scl
RUN yum install -y llvm-toolset-7
RUN yum install -y gcc make zlib-devel curl-devel
RUN rpm -Uvh http://download-ib01.fedoraproject.org/pub/epel/7/x86_64/Packages/l/llvm5.0-libs-5.0.1-7.el7.x86_64.rpm
RUN rpm -Uvh http://download-ib01.fedoraproject.org/pub/epel/7/x86_64/Packages/l/llvm5.0-5.0.1-7.el7.x86_64.rpm

ENV PATH /usr/pgsql-11/bin:$PATH
