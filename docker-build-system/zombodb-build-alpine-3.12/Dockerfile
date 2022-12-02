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
ARG PGVER
FROM postgres:${PGVER}-alpine

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

ARG USER=docker
ARG UID=1000
ARG GID=1000


RUN adduser --uid=${UID} -D ${USER}

ENV RUSTFLAGS="-C target-feature=-crt-static"

RUN apk add --no-cache \
    git \
	curl \
	bash \
	ruby-dev \
	ruby-etc \
	musl-dev \
	make \
	gcc \
	coreutils \
	util-linux-dev \
	musl-dev \
	openssl-dev \
	clang-libs \
	tar

RUN gem install --no-document fpm

WORKDIR /home/${USER}
USER ${USER}

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/home/${USER}/.cargo/bin:${PATH}"

RUN cargo install cargo-pgx

ARG PGVER
RUN cargo pgx init --pg${PGVER}=$(which pg_config)
