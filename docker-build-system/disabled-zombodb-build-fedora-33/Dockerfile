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
FROM fedora:33

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

ARG USER=docker
ARG UID=1000
ARG GID=1000


RUN useradd -m ${USER} --uid=${UID}

RUN dnf update -y
RUN dnf install -y git
RUN dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/F-33-x86_64/pgdg-fedora-repo-latest.noarch.rpm
RUN dnf install -y postgresql10-server postgresql10-devel
RUN dnf install -y postgresql11-server postgresql11-devel
RUN dnf install -y postgresql12-server postgresql12-devel
RUN dnf install -y postgresql13-server postgresql13-devel
RUN dnf install -y postgresql14-server postgresql14-devel
RUN dnf group install -y "Development Tools"
RUN dnf install -y openssl-devel
RUN dnf install -y redhat-rpm-config
RUN dnf install -y ruby-devel gcc make rpm-build libffi-devel
RUN dnf update -y
RUN gem install --no-document fpm


USER ${UID}:${GID}
WORKDIR /home/${USER}
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/home/${USER}/.cargo/bin:${PATH}"

RUN cargo install cargo-pgx
RUN cargo pgx init \
	--pg10=/usr/pgsql-10/bin/pg_config \
	--pg11=/usr/pgsql-11/bin/pg_config \
	--pg12=/usr/pgsql-12/bin/pg_config \
	--pg13=/usr/pgsql-13/bin/pg_config \
	--pg14=/usr/pgsql-14/bin/pg_config
