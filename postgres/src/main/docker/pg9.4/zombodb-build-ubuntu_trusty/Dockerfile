FROM ubuntu:trusty

MAINTAINER ZomboDB, LLC (zombodb@gmail.com)

RUN apt-get update -y -qq --fix-missing
RUN apt-get install -y wget
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main" >> /etc/apt/sources.list.d/pgdg.list
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update -y --fix-missing
RUN apt-get install -y postgresql-server-dev-9.4
RUN apt-get install -y gcc make build-essential libz-dev zlib1g-dev strace

COPY curl-7.53.1.tar.gz /usr/local/src/
WORKDIR /usr/local/src/
RUN tar xzf curl-7.53.1.tar.gz
WORKDIR /usr/local/src/curl-7.53.1/
RUN ./configure --without-ssl \
   --disable-threaded-resolver \
   --disable-dict \
   --disable-file \
   --disable-ftp \
   --disable-gopher \
   --disable-imap \
   --disable-pop3 \
   --disable-rtsp \
   --disable-smb \
   --disable-smtp \
   --disable-telnet \
   --disable-tftp
RUN make -j2 && make install
RUN ldconfig && ldconfig
RUN rm -rf /usr/local/src/curl*

ARG user
ARG uid
RUN useradd -ms /bin/bash --uid $uid $user
USER $user
ENV IS_DOCKER true
