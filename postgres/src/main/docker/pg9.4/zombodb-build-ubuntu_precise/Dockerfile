FROM ubuntu:precise

MAINTAINER ZomboDB, LLC (zombodb@gmail.com)

RUN apt-get update -y -qq --fix-missing
RUN apt-get install -y wget
RUN apt-get install -y apt-utils
RUN echo "deb http://apt.postgresql.org/pub/repos/apt/ precise-pgdg main" >> /etc/apt/sources.list.d/pgdg.list
RUN wget --no-check-certificate --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN apt-get update -y --fix-missing
RUN apt-get install -y --force-yes postgresql-server-dev-9.4
RUN apt-get install -y gcc make build-essential

COPY curl-7.43.0.tar.gz /usr/local/src/
WORKDIR /usr/local/src/
RUN tar xzf curl-7.43.0.tar.gz
WORKDIR /usr/local/src/curl-7.43.0/
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
