#
# Copyright 2018-2022 ZomboDB, LLC
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
FROM amazonlinux:2

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

ARG USER=docker
ARG UID=1000
ARG GID=1000


RUN yum install -y shadow-utils
RUN useradd -m ${USER} --uid=${UID}

RUN yum install -y yum-utils which procps psacct psmisc
RUN yum makecache
RUN yum install -y git gcc make rpm-build libffi-devel
RUN yum install -y autoconf automake bison libtool readline-devel ruby sqlite-devel libyaml-devel
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm
RUN yum install -y clang llvm5.0-devel openssl-devel

RUN echo $'\n\
[pgdg10]\n\
name=PostgreSQL 10 for RHEL/CentOS 7 - x86_64\n\
baseurl=https://download.postgresql.org/pub/repos/yum/10/redhat/rhel-7-x86_64\n\
enabled=1\n\
gpgcheck=0\n\
\n\
[pgdg11]\n\
name=PostgreSQL 11 for RHEL/CentOS 7 - x86_64\n\
baseurl=https://download.postgresql.org/pub/repos/yum/11/redhat/rhel-7-x86_64\n\
enabled=1\n\
gpgcheck=0\n\
\n\
[pgdg12]\n\
name=PostgreSQL 12 for RHEL/CentOS 7 - x86_64\n\
baseurl=https://download.postgresql.org/pub/repos/yum/12/redhat/rhel-7-x86_64\n\
enabled=1\n\
gpgcheck=0\n\
\n\
[pgdg13]\n\
name=PostgreSQL 13 for RHEL/CentOS 7 - x86_64\n\
baseurl=https://download.postgresql.org/pub/repos/yum/13/redhat/rhel-7-x86_64\n\
enabled=1\n\
gpgcheck=0\n\
\n\
[pgdg14]\n\
name=PostgreSQL 14 for RHEL/CentOS 7 - x86_64\n\
baseurl=https://download.postgresql.org/pub/repos/yum/14/redhat/rhel-7-x86_64\n\
enabled=1\n\
gpgcheck=0\n\
' >> /etc/yum.repos.d/pgdg.repo


RUN yum install -y http://mirror.centos.org/centos/7/extras/x86_64/Packages/centos-release-scl-rh-2-3.el7.centos.noarch.rpm
RUN yum install -y llvm-toolset-7-clang
RUN yum install -y postgresql10 postgresql10-server postgresql10-devel
RUN yum install -y postgresql11 postgresql11-server postgresql11-devel
RUN yum install -y postgresql12 postgresql12-server postgresql12-devel
RUN yum install -y postgresql13 postgresql13-server postgresql13-devel
RUN yum install -y postgresql14 postgresql14-server postgresql14-devel

USER ${UID}:${GID}
WORKDIR /home/${USER}

# install ruby and fpm
RUN curl -sSL https://rvm.io/mpapis.asc | gpg2 --import -
RUN curl -sSL https://rvm.io/pkuczynski.asc | gpg2 --import -
RUN curl -sSL https://get.rvm.io | bash -s stable
RUN /bin/bash -l -c ". ~/.rvm/scripts/rvm && rvm install 2.3.3"
RUN /bin/bash -l -c ". ~/.rvm/scripts/rvm && gem install --no-document fpm"


RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/home/${USER}/.cargo/bin:${PATH}"

RUN cargo install cargo-pgx
RUN cargo pgx init \
	--pg10=/usr/pgsql-10/bin/pg_config \
	--pg11=/usr/pgsql-11/bin/pg_config \
	--pg12=/usr/pgsql-12/bin/pg_config \
	--pg13=/usr/pgsql-13/bin/pg_config \
	--pg14=/usr/pgsql-14/bin/pg_config
