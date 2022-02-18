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
FROM centos:6

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

ARG USER=docker
ARG UID=1000
ARG GID=1000

RUN useradd -m ${USER} --uid=${UID}

RUN yum install -y git
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-6-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum install -y postgresql12-server postgresql12-devel
RUN yum install -y postgresql11-server postgresql11-devel
RUN yum install -y postgresql10-server postgresql10-devel
RUN yum groupinstall -y "Development Tools"
RUN yum install -y openssl-devel

USER ${UID}:${GID}
WORKDIR /home/${USER}
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/home/${USER}/.cargo/bin:${PATH}"

RUN cargo install cargo-pgx
RUN cargo pgx init \
	--pg10=/usr/pgsql-10/bin/pg_config \
	--pg11=/usr/pgsql-11/bin/pg_config \
	--pg12=/usr/pgsql-12/bin/pg_config
