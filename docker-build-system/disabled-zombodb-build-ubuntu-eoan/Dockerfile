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
FROM ubuntu:eoan

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

ARG USER=docker
ARG UID=1000
ARG GID=1000


RUN useradd -m ${USER} --uid=${UID}

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y -qq --fix-missing
RUN apt-get install -y wget gnupg
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ eoan-pgdg main" >> /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt update
RUN apt-get update -y --fix-missing

RUN apt-get install -y git
RUN apt-get install -y curl
RUN apt-get install -y clang-9 llvm-9 gcc make build-essential libz-dev zlib1g-dev strace libssl-dev pkg-config
RUN apt-get install -y postgresql-10 postgresql-server-dev-10
RUN apt-get install -y postgresql-11 postgresql-server-dev-11
RUN apt-get install -y postgresql-12 postgresql-server-dev-12
RUN apt-get install -y postgresql-13 postgresql-server-dev-13
RUN apt-get install -y ruby ruby-dev rubygems build-essential
RUN gem install --no-document fpm


USER ${UID}:${GID}
WORKDIR /home/${USER}
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/home/${USER}/.cargo/bin:${PATH}"

RUN cargo install cargo-pgx
RUN cargo pgx init \
	--pg10=/usr/lib/postgresql/10/bin/pg_config \
	--pg11=/usr/lib/postgresql/11/bin/pg_config \
	--pg12=/usr/lib/postgresql/12/bin/pg_config \
	--pg13=/usr/lib/postgresql/13/bin/pg_config
