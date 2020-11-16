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
FROM alpine:3.12 as build

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

ARG USER=docker
ARG UID=1000
ARG GID=1000
RUN adduser --uid=${UID} -D ${USER}

RUN apk add --no-cache \
	musl-dev \
	openssl-dev \
	make \
	bison \
	coreutils \
	dpkg-dev dpkg \
	flex \
	gcc \
	libc-dev \
	libedit-dev \
	libxml2-dev \
	libxslt-dev \
	linux-headers \
	make \
	openssl-dev \
	perl-utils \
	perl-ipc-run \
	util-linux-dev \
	zlib-dev \
	icu-dev \
	curl

WORKDIR /home/${USER}

FROM build as postgresql

ARG USER=docker
USER ${USER}

COPY build_pg.sh build_pg.sh

ENV PG_10=10.15
RUN wget https://ftp.postgresql.org/pub/source/v${PG_10}/postgresql-${PG_10}.tar.gz -qO /tmp/postgresql-10.tar.gz
RUN sh ./build_pg.sh 10

ENV PG_11=11.10
RUN wget https://ftp.postgresql.org/pub/source/v${PG_11}/postgresql-${PG_11}.tar.gz -qO /tmp/postgresql-11.tar.gz
RUN sh ./build_pg.sh 11

ENV PG_12=12.5
RUN wget https://ftp.postgresql.org/pub/source/v${PG_12}/postgresql-${PG_12}.tar.gz -qO /tmp/postgresql-12.tar.gz
RUN sh ./build_pg.sh 12

ENV PG_13=13.1
RUN wget https://ftp.postgresql.org/pub/source/v${PG_13}/postgresql-${PG_13}.tar.gz -qO /tmp/postgresql-13.tar.gz
RUN sh ./build_pg.sh 13

FROM build

RUN apk add --no-cache clang-libs bash ruby-dev ruby-etc

RUN gem install --no-document fpm

ARG USER=docker
USER ${USER}

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/home/${USER}/.cargo/bin:${PATH}"

RUN cargo install cargo-pgx

COPY --from=postgresql /home/${USER}/postgresql /home/${USER}/postgresql

RUN cargo pgx init \
	--pg10=/home/${USER}/postgresql/pg10/build/bin/pg_config \
	--pg11=/home/${USER}/postgresql/pg11/build/bin/pg_config \
	--pg12=/home/${USER}/postgresql/pg12/build/bin/pg_config \
	--pg13=/home/${USER}/postgresql/pg13/build/bin/pg_config

ENV RUSTFLAGS="-C target-feature=-crt-static"
