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
FROM debian:sid

LABEL maintainer="ZomboDB, LLC (zombodb@gmail.com)"

ARG DEBIAN_FRONTEND=noninteractive
RUN apt-get update -y -qq --fix-missing
RUN apt-get install -y wget gnupg
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ sid-pgdg main" >> /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update -y --fix-missing
RUN apt-get install -y curl
RUN apt-get install -y gcc make build-essential libz-dev zlib1g-dev strace libssl-dev pkg-config
RUN apt-get install -y postgresql-12 postgresql-server-dev-12
RUN apt-get install -y postgresql-11 postgresql-server-dev-11
RUN apt-get install -y postgresql-10 postgresql-server-dev-10
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"
RUN cargo install cargo-pgx
RUN cargo pgx init \
	--pg10=/usr/lib/postgresql/10/bin/pg_config \
	--pg11=/usr/lib/postgresql/11/bin/pg_config \
	--pg12=/usr/lib/postgresql/12/bin/pg_config
